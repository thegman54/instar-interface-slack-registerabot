"""Slack → RegisterABot adapter (multi-tenant).

A THIN bridge, not a full connector. It receives Slack messages (and files) via Socket Mode
and forwards them to a bot over the RegisterABot relay as a *service client*.

Two tokens, do not confuse them:
  - This adapter's OWN service key (SLACK_REGISTERABOT_TOKEN) — its identity as a SERVICE on
    the relay, used to send service→bot (/ws/service/{svc}?key=…&bot=…). Lives in THIS
    adapter's Infisical.
  - The bot's key lives in the PROFILE (its registerabot binding) and is used by the instar
    registerabot connector for the bot to connect as a BOT — none of this adapter's business.

WHICH bot we route to is NOT ours to decide. The adapter owns only its service identity; the
*bot slug comes from the instar profile* that has this interface checked. On profile launch the
gatekeeper calls POST /slugs/{bot_slug}/connect (the standard multi-tenant interface contract) —
that's how the profile hands us its registerabot bot slug; on stop, /slugs/{bot}/disconnect.
There is deliberately no BOT_SLUG in this adapter's config.

Two directions:
  - INBOUND  — Socket Mode event -> relay chat_request -> bot; frames come back and get
    posted into the mapped thread.
  - OUTBOUND — POST /outbound on the control plane opens a DM with someone the bot has never
    spoken to and posts into it (optionally with a native call block). Reachable only from
    instar-internal, i.e. via tool-executor; the bot cannot call it directly.

Creds (from Infisical):
  - Slack: SLACK_BOT_TOKEN + SLACK_APP_TOKEN — receive events, download url_private files,
    upload outbound files/audio/video.
  - Relay identity (OURS): SLACK_REGISTERABOT_SERVICE_SLUG (who we speak AS) +
    SLACK_REGISTERABOT_TOKEN (our service key).
"""

import asyncio
import base64
import json
import os
import threading
import re
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import httpx
import structlog
import websockets
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler

structlog.configure(processors=[structlog.processors.TimeStamper(fmt="iso"),
                                structlog.dev.ConsoleRenderer()])
log = structlog.get_logger()

# --- creds / config (injected from Infisical by the stack) --------------------
SLACK_BOT_TOKEN = os.environ.get("SLACK_BOT_TOKEN", "")
SLACK_APP_TOKEN = os.environ.get("SLACK_APP_TOKEN", "")
RELAY_URL = os.environ.get("REGISTERABOT_RELAY_URL", "").rstrip("/")   # wss://relay… (shared)
# This adapter's OWN relay identity (a SERVICE). Its own slug + own key — NOT the profile's.
SERVICE_SLUG = (os.environ.get("SLACK_REGISTERABOT_SERVICE_SLUG")
                or os.environ.get("REGISTERABOT_SERVICE_SLUG", "slack-adapter"))
SERVICE_KEY = os.environ.get("SLACK_REGISTERABOT_TOKEN", "")           # our service key
CONTROL_PORT = int(os.environ.get("CONTROL_PORT", "8092"))   # instar multi-tenant connect/disconnect
SESSION_TTL = int(os.environ.get("SESSION_TTL", "600"))

# How long a transport may stay down before the watchdog kills the process so
# `restart: unless-stopped` brings up a clean one. An unhealthy healthcheck does NOT
# restart a container by itself — reporting a fault is not recovering from it. On
# 2026-08-19 this adapter sat "healthy" for ~7h with a dead Slack socket, receiving
# nothing. Long enough to ride out ordinary reconnects, short enough to not lose a morning.
WATCHDOG_GRACE = int(os.environ.get("WATCHDOG_GRACE", "180"))

# --- relay + routing state ----------------------------------------------------
_relay_ws = None
_relay_loop: asyncio.AbstractEventLoop | None = None
_bot_user_id: str | None = None
_active_bot: str | None = None            # which bot we route TO — comes from the profile
_socket_handler = None                    # SocketModeHandler — the ONLY inbound path from Slack
_relay_down_since: float | None = None    # when the relay WS dropped (None = up, or idle by design)
# session_id -> {"channel", "thread_ts", "ts"} — where to post this turn's frames
_sessions: dict[str, dict] = {}


def _slack_connected() -> bool:
    """Is the Socket Mode connection actually up?

    This is the ONLY way Slack messages reach us. `SocketModeHandler.client.is_connected()`
    reflects the live WebSocket, not the fact that a process is running and a port is open —
    which is the distinction that matters and the one the old healthcheck missed entirely.
    """
    try:
        return bool(_socket_handler and _socket_handler.client.is_connected())
    except Exception:
        return False


def _health() -> tuple[bool, dict]:
    """(ok, detail). Unhealthy means messages cannot flow, in either direction.

    - Slack socket down  -> nothing can reach us. Always unhealthy.
    - Relay down WITH an active bot -> we can hear but cannot answer. Unhealthy.
    - Relay idle with no bot connected -> correct behaviour, not a fault.
    """
    slack_ok = _slack_connected()
    relay_ok = _relay_ws is not None
    idle = _active_bot is None
    ok = slack_ok and (relay_ok or idle)
    detail = {
        "status": "ok" if ok else "degraded",
        "service": SERVICE_SLUG,
        "active_bot": _active_bot,
        "slack_socket": "connected" if slack_ok else "disconnected",
        "relay": "connected" if relay_ok else ("idle" if idle else "disconnected"),
    }
    if not ok:
        detail["reason"] = ("slack socket mode is down — no messages can arrive"
                            if not slack_ok else
                            f"relay is down while bot '{_active_bot}' is connected — cannot reply")
    return ok, detail


def _watchdog():
    """Kill the process when a transport stays down past the grace period.

    Docker does not restart an unhealthy container; `restart: unless-stopped` only reacts to
    the process EXITING. So recovery has to be an exit. Both transports self-heal on their own
    first — this only fires when that has demonstrably failed for WATCHDOG_GRACE seconds.
    """
    down_since: float | None = None
    while True:
        time.sleep(15)
        ok, detail = _health()
        if ok:
            down_since = None
            continue
        now = time.time()
        if down_since is None:
            down_since = now
            log.warning("transport_down", **detail)
            continue
        if now - down_since >= WATCHDOG_GRACE:
            log.error("watchdog_restart", down_for=int(now - down_since), **detail)
            os._exit(1)   # hard exit: the supervisor gives us a clean process


def _prune_sessions():
    cutoff = time.time() - SESSION_TTL
    for sid in [s for s, v in _sessions.items() if v.get("ts", 0) < cutoff]:
        _sessions.pop(sid, None)


def _session_key(channel: str, user_id: str, thread_ts: str | None) -> str:
    """STABLE conversation key — never a fresh uuid per message.

    session_id becomes the gatekeeper's conversation_id (`registerabot:{sid}`), which is what
    groups multi-turn context AND what counts against a bot's session budget. Minting a uuid
    per message meant every Slack line started a brand-new conversation: no memory of the last
    turn, and the bot's session count drained one message at a time.

    Keying:
      - a reply inside a real thread  -> per-THREAD  (everyone in the thread shares context)
      - a top-level message           -> per-USER in that channel/DM (Alex and Ross each get
        their own rolling conversation instead of colliding or forking every message)
    """
    if thread_ts:
        return f"slack-{channel}-t{thread_ts}"
    return f"slack-{channel}-u{user_id}"


# --- Slack posting (blocking SDK calls → run off the relay loop) --------------
def _post_text(channel: str, thread_ts: str, text: str):
    try:
        slack_app.client.chat_postMessage(channel=channel, thread_ts=thread_ts, text=text)
    except Exception as e:
        log.warning("slack_post_failed", error=str(e))


def _upload_file(channel: str, thread_ts: str, data: bytes, filename: str, title: str = ""):
    """Upload bytes as a real Slack file. Slack renders mp3/mp4 with an inline player."""
    try:
        slack_app.client.files_upload_v2(channel=channel, thread_ts=thread_ts,
                                         content=data, filename=filename, title=title or filename)
        log.info("slack_file_uploaded", filename=filename, bytes=len(data))
    except Exception as e:
        log.warning("slack_upload_failed", filename=filename, error=str(e))


def _b64(x: str) -> bytes | None:
    try:
        return base64.b64decode(x)
    except Exception:
        return None


async def _resolve_bytes(item: dict) -> bytes | None:
    """Bytes for a media item, however the relay delivered it: inline base64
    (`data`/`base64`/`audio_base64`) or a hosted `url` (fetch it)."""
    for k in ("data", "base64", "audio_base64"):
        v = item.get(k)
        if v:
            b = _b64(v)
            if b is not None:
                return b
    url = item.get("url")
    if url:
        try:
            async with httpx.AsyncClient() as c:
                r = await c.get(url, timeout=30)
            if r.status_code == 200:
                return r.content
            log.warning("ref_fetch_status", url=url, status=r.status_code)
        except Exception as e:
            log.warning("ref_fetch_failed", url=url, error=str(e))
    return None


async def _dispatch_frame(env: dict):
    """A bot→service frame arrived on the relay. Post it into the mapped Slack thread."""
    dest = _sessions.get(env.get("session_id"))
    if not dest:
        return
    channel, thread = dest["channel"], dest["thread_ts"]
    ftype = env.get("type")
    loop = asyncio.get_running_loop()
    try:
        payload = json.loads(env.get("payload") or "{}")
    except Exception:
        payload = {}

    if ftype == "final_response":
        reply = payload.get("reply") or ""
        if reply:
            await loop.run_in_executor(None, _post_text, channel, thread, reply)
        for a in payload.get("attachments") or []:
            if a.get("type") == "audio":
                continue
            data = await _resolve_bytes(a)
            if data:
                name = a.get("name", "file")
                await loop.run_in_executor(None, _upload_file, channel, thread, data, name, name)
        audio = payload.get("audio") or {}
        if audio:
            adata = await _resolve_bytes(audio)
            if adata:
                await loop.run_in_executor(None, _upload_file, channel, thread, adata,
                                           "voice.mp3", "Voice")
    elif ftype == "video":
        vdata = await _resolve_bytes(payload)
        if vdata:
            # A turn may arrive as several clips (payload carries seq/final — see the
            # chunked renderer). Slack has no way to stitch them, so they land as separate
            # files; number them or they all show up as an indistinguishable "avatar.mp4"
            # and the reader can't tell what order to play them in.
            seq = payload.get("seq")
            final = payload.get("final", True)
            chunked = seq is not None and not (seq == 0 and final)
            name = f"avatar-{int(seq) + 1}.mp4" if chunked else "avatar.mp4"
            label = f"Avatar ({int(seq) + 1})" if chunked else "Avatar"
            await loop.run_in_executor(None, _upload_file, channel, thread, vdata,
                                       name, label)
    # 'audio' (duplicate of embedded) and 'idle' frames are intentionally ignored.


async def _relay_client():
    """Maintain the service-client WS to the relay for whichever bot the profile connected us
    to. We authenticate with OUR OWN service slug + key; the bot slug is the profile's.
    Reconnects when _active_bot changes (connect/disconnect close the socket)."""
    global _relay_ws, _relay_down_since
    while True:
        bot = _active_bot
        if not bot:
            await asyncio.sleep(1)   # idle until a profile connects us
            continue
        url = f"{RELAY_URL}/ws/service/{SERVICE_SLUG}?key={SERVICE_KEY}&bot={bot}"
        try:
            async with websockets.connect(url, max_size=None) as ws:
                _relay_ws = ws
                _relay_down_since = None
                log.info("relay_connected", service=SERVICE_SLUG, bot=bot)
                async for raw in ws:
                    if _active_bot != bot:   # profile re-pointed us — drop and reconnect
                        break
                    try:
                        env = json.loads(raw)
                    except json.JSONDecodeError:
                        continue
                    if env.get("session_id"):
                        try:
                            await _dispatch_frame(env)
                        except Exception as e:
                            log.warning("dispatch_error", error=str(e))
        except Exception as e:
            log.warning("relay_disconnected", error=str(e))
        finally:
            _relay_ws = None
            if _relay_down_since is None:
                _relay_down_since = time.time()
        await asyncio.sleep(2)


async def _send_to_bot(sid: str, text: str, user_id: str, attachments: list):
    """Send one chat_request over the relay to the currently-connected bot."""
    if _relay_ws is None or not _active_bot:
        # Was a silent return. A Slack message would arrive, hit this line, and vanish with no
        # log anywhere — indistinguishable from the bot being down, and the cause of an hour
        # spent looking at Slack scopes when the break was here.
        log.warning("send_to_bot_skipped",
                    reason="no relay socket" if _relay_ws is None else "no active bot",
                    active_bot=_active_bot, session=sid, chars=len(text))
        return
    envelope = {
        "v": 1, "type": "chat_request", "session_id": sid,
        "timestamp": int(time.time() * 1000),
        "from": {"kind": "service", "slug": SERVICE_SLUG, "name": "Slack"},
        "to": {"kind": "bot", "slug": _active_bot},
        "encrypted": False,
        # Attachments go ON THE MESSAGE — the bot-side registerabot connector reads
        # m.get('attachments') per message and forwards them to /process (on_file barrier).
        "payload": json.dumps({
            "messages": [{"role": "user", "content": text, "attachments": attachments}],
            "user_context": {"user_id": user_id},
        }),
    }
    try:
        await _relay_ws.send(json.dumps(envelope))
        log.info("sent_to_bot", bot=_active_bot, session=sid, chars=len(text),
                 attachments=len(attachments or []))
    except Exception as e:
        log.warning("send_to_bot_failed", error=str(e), bot=_active_bot, session=sid)


def _fetch_slack_files(files: list) -> list:
    """Download Slack files (url_private needs the Slack bot token) → attachment dicts."""
    out = []
    for f in files or []:
        url = f.get("url_private_download") or f.get("url_private")
        if not url:
            continue
        try:
            r = httpx.get(url, headers={"Authorization": f"Bearer {SLACK_BOT_TOKEN}"}, timeout=30)
            if r.status_code == 200:
                out.append({
                    "name": f.get("name", "file"),
                    "mime": f.get("mimetype", ""),
                    "encoding": "base64",
                    "data": base64.b64encode(r.content).decode(),
                })
                log.info("slack_file_fetched", name=f.get("name"), bytes=len(r.content))
        except Exception as e:
            log.warning("slack_file_fetch_failed", error=str(e))
    return out


# --- outbound: start a conversation Slack-side instead of only answering one ----
def _resolve_recipient(target: str) -> tuple[str, str]:
    """A human-supplied recipient -> (channel_id, user_id).

    Accepts, in order of directness:
      - a channel/DM id (C…/D…/G…) -> used as-is, user_id unknown ("")
      - a user id (U…/W…)          -> conversations.open to get the DM channel
      - an email                   -> users.lookupByEmail  (needs users:read.email)
      - a name / @name             -> scan users.list      (needs users:read)

    conversations.open is what makes a COLD dm possible: it creates the DM channel with
    someone the bot has never spoken to. Posting to a raw U… id also works, but opening
    explicitly gives us the real channel id to key the session on.
    """
    t = (target or "").strip().lstrip("@")
    if not t:
        raise ValueError("no recipient")

    if t[0] in ("C", "D", "G") and t[1:].isalnum() and t.isupper():
        return t, ""

    user_id = ""
    if t[0] in ("U", "W") and t.isupper() and t[1:].isalnum():
        user_id = t
    elif "@" in t and "." in t.split("@")[-1]:
        user_id = slack_app.client.users_lookupByEmail(email=t)["user"]["id"]
    else:
        needle = t.lower()
        cursor = None
        while True:
            page = slack_app.client.users_list(limit=200, cursor=cursor)
            for u in page.get("members", []):
                if u.get("deleted") or u.get("is_bot"):
                    continue
                prof = u.get("profile") or {}
                names = {u.get("name", ""), u.get("real_name", ""),
                         prof.get("display_name", ""), prof.get("real_name", "")}
                if needle in {n.lower() for n in names if n}:
                    user_id = u["id"]
                    break
            if user_id:
                break
            cursor = (page.get("response_metadata") or {}).get("next_cursor")
            if not cursor:
                raise ValueError(f"no Slack user matches '{target}'")

    channel = slack_app.client.conversations_open(users=user_id)["channel"]["id"]
    return channel, user_id


def _post_call_block(channel: str, join_url: str, title: str) -> str | None:
    """Mint a Slack call object and post it as a native call block. Returns the call id.

    calls.add only CREATES the object — it rings nobody and delivers nothing. The message
    below is the entire notification. Slack has no API to start or join a huddle, so this
    block plus our own media path is as close as a bot gets to calling someone.
    """
    call = slack_app.client.calls_add(
        external_unique_id=uuid.uuid4().hex,
        join_url=join_url,
        created_by=_bot_user_id,
        title=title or "Call",
    )
    call_id = call["call"]["id"]
    slack_app.client.chat_postMessage(
        channel=channel, text=title or "Call",
        blocks=[{"type": "call", "call_id": call_id}],
    )
    return call_id


def _outbound(body: dict) -> tuple[int, dict]:
    """Open a conversation with a Slack user and post to it.

    Everything the adapter did until now was reactive: it could only answer inside a session
    an inbound message had created. This is the other direction.

    The reply path is unchanged — we seed `_sessions` with the SAME key the inbound handler
    would compute, so when the human answers, their message lands in this conversation
    instead of forking a new one.
    """
    bot = body.get("bot")
    if not _active_bot:
        return 409, {"error": "no bot is connected to this adapter"}
    if bot and bot != _active_bot:
        return 409, {"error": f"bot '{bot}' is not the connected bot"}

    text = (body.get("text") or "").strip()
    join_url = (body.get("join_url") or "").strip()
    if not text and not join_url:
        return 400, {"error": "text or join_url is required"}

    try:
        channel, user_id = _resolve_recipient(body.get("to") or "")
    except Exception as e:
        return 400, {"error": f"could not resolve recipient: {e}"}

    result = {"status": "sent", "channel": channel, "user": user_id, "bot": _active_bot}
    try:
        if text:
            posted = slack_app.client.chat_postMessage(channel=channel, text=text)
            result["ts"] = posted.get("ts")
        if join_url:
            try:
                result["call_id"] = _post_call_block(channel, join_url, body.get("title") or "")
            except Exception as e:
                # calls:write missing, or the call object was rejected. A plain link still
                # gets the human into the room — degrade instead of failing the send.
                log.warning("calls_add_failed", error=str(e))
                slack_app.client.chat_postMessage(
                    channel=channel, text=f"Join: {join_url}")
                result["call_block"] = f"unavailable ({e}) — posted a plain link"
    except Exception as e:
        log.warning("outbound_post_failed", error=str(e))
        return 502, {"error": f"slack post failed: {e}"}

    # Key it exactly as an inbound top-level message from this user would be keyed, so their
    # reply continues THIS conversation. thread_ts is left unset: the human answers in the DM
    # at top level, and the inbound handler will pin the thread when they do.
    sid = _session_key(channel, user_id or "slack", None)
    _prune_sessions()
    _sessions[sid] = {"channel": channel, "thread_ts": result.get("ts"), "ts": time.time()}
    result["session_id"] = sid

    log.info("outbound_sent", channel=channel, user=user_id, bot=_active_bot,
             has_call=bool(join_url), session_id=sid)
    return 200, result


# --- multi-tenant control plane (instar connects/disconnects us per profile) --
def _set_active_bot(bot: str | None):
    """Point (or unpoint) the adapter at a bot slug (from the profile). Only force a reconnect
    when the bot actually CHANGES — the gatekeeper keepalive re-calls connect on every cycle
    with the same bot, and churning the socket each time would drop the connection for ~2s
    (during which a message would wrongly get 'No bot connected')."""
    global _active_bot
    if bot == _active_bot:
        return  # no change — leave the live socket alone
    _active_bot = bot
    if _relay_loop and _relay_ws is not None:
        # Close the current socket so _relay_client reconnects with the new bot (or idles).
        asyncio.run_coroutine_threadsafe(_relay_ws.close(), _relay_loop)


class _Control(BaseHTTPRequestHandler):
    def _reply(self, code, obj):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path in ("/health", "/status"):
            # 503 when messages cannot flow. A 200 here used to mean nothing more than
            # "the control server is listening", which is true of a completely deaf adapter.
            ok, detail = _health()
            return self._reply(200 if ok else 503, detail)
        self._reply(404, {"error": "not found"})

    def _body(self) -> dict:
        try:
            n = int(self.headers.get("Content-Length") or 0)
            return json.loads(self.rfile.read(n)) if n else {}
        except Exception:
            return {}

    def do_POST(self):
        parts = [p for p in self.path.split("/") if p]  # ['slugs', '{bot}', 'connect']

        # Outbound is reachable only from instar-internal — the same trust boundary that
        # already governs connect/disconnect. The bot itself is NOT on that network; it
        # gets here through mcp-server -> tool-executor, which is the point.
        if parts == ["outbound"]:
            code, obj = _outbound(self._body())
            return self._reply(code, obj)

        # The profile hands us ONLY its bot slug (in the path). We authenticate as our own
        # service; the profile's own token is the bot's, not ours, so we ignore any body.
        if len(parts) == 3 and parts[0] == "slugs":
            bot, action = parts[1], parts[2]
            try:
                n = int(self.headers.get("Content-Length") or 0)
                if n:
                    self.rfile.read(n)   # drain body; we don't use it
            except Exception:
                pass
            if action == "connect":
                _set_active_bot(bot)
                log.info("profile_connected", bot=bot, service=SERVICE_SLUG)
                return self._reply(200, {"status": "connected", "bot": bot, "service": SERVICE_SLUG})
            if action == "disconnect":
                if _active_bot == bot:
                    _set_active_bot(None)
                log.info("profile_disconnected", bot=bot)
                return self._reply(200, {"status": "disconnected", "bot": bot})
        self._reply(404, {"error": "not found"})

    def log_message(self, *a):   # silence default stderr logging
        pass


def _start_control_server():
    # Threaded: an outbound send makes 2-3 blocking Slack calls, and a single-threaded
    # server would stall /health behind them long enough to look dead to the healthcheck.
    ThreadingHTTPServer(("0.0.0.0", CONTROL_PORT), _Control).serve_forever()


# --- Slack Socket Mode ---------------------------------------------------------
slack_app = App(token=SLACK_BOT_TOKEN)


def _handle_incoming(event, say):
    """One path for a human message, whichever event carried it.

    Shared by `message` and `app_mention` rather than duplicated. The two events overlap
    confusingly — a mention in a channel the app can read arrives as BOTH — and two copies of
    this logic would drift into answering one and not the other.
    """
    if event.get("bot_id") or event.get("user") == _bot_user_id:
        return
    if event.get("subtype") in ("message_changed", "message_deleted", "channel_join"):
        return

    # Strip the leading @bot. Slack delivers "<@U0AFXK4SXSL> do the thing", and passing the
    # raw id through means the bot reads its own user id as the first word of every request.
    text = re.sub(r"<@[A-Z0-9]+>", "", event.get("text") or "").strip()
    files = event.get("files", [])
    if not text and not files:
        return

    thread_ts = event.get("thread_ts") or event.get("ts")
    if not _active_bot or _relay_ws is None:
        say(text="No bot is connected to this Slack workspace yet.", thread_ts=thread_ts)
        return

    attachments = _fetch_slack_files(files)
    user_id = event.get("user", "slack")
    channel = event.get("channel", "")

    sid = _session_key(channel, user_id, event.get("thread_ts"))
    _prune_sessions()
    _sessions[sid] = {"channel": channel, "thread_ts": thread_ts, "ts": time.time()}
    log.info("slack_message_received", channel=channel, user=user_id,
             chars=len(text), files=len(files))
    if _relay_loop is None:
        log.error("relay_loop_missing", session=sid,
                  hint="the relay thread never started; nothing can be forwarded")
        say(text="I received that but my transport is not running — nothing was sent.",
            thread_ts=thread_ts)
        return

    fut = asyncio.run_coroutine_threadsafe(
        _send_to_bot(sid, text or "(file attached)", user_id, attachments), _relay_loop)

    # Read the Future. Dropping it swallows every exception raised inside the coroutine,
    # which is how a message could be received, fail to forward, and leave no trace at all.
    def _report(f):
        try:
            f.result()
        except Exception as exc:
            log.error("forward_failed", session=sid, error=str(exc)[:200])
    fut.add_done_callback(_report)


@slack_app.event("message")
def on_slack_message(event, say):
    _handle_incoming(event, say)


# Channel messages need `channels:history`, which this app does NOT have — it has only
# `im:history`, so a message posted in a channel never reaches the handler above and simply
# vanishes. `app_mentions:read` IS granted, and an @mention arrives as its own event type,
# which nothing was listening for.
#
# So without this handler, being @mentioned in a channel did nothing at all and looked
# identical to the bot being down. Adding it makes channel mentions work with the scopes
# already granted, rather than waiting on a Slack app reinstall.
@slack_app.event("app_mention")
def on_slack_app_mention(event, say):
    _handle_incoming(event, say)


def _start_relay_thread():
    global _relay_loop
    _relay_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(_relay_loop)
    _relay_loop.run_until_complete(_relay_client())


def main():
    global _bot_user_id, _socket_handler
    # Required: Slack platform creds, relay URL, and OUR OWN service key (SLACK_REGISTERABOT_TOKEN).
    # The bot slug is NOT required — it arrives from the profile via /slugs/{bot}/connect.
    missing = [k for k, v in {
        "SLACK_BOT_TOKEN": SLACK_BOT_TOKEN, "SLACK_APP_TOKEN": SLACK_APP_TOKEN,
        "REGISTERABOT_RELAY_URL": RELAY_URL, "SLACK_REGISTERABOT_TOKEN": SERVICE_KEY,
    }.items() if not v]
    if missing:
        raise SystemExit(f"Missing required env: {missing}")

    try:
        _bot_user_id = slack_app.client.auth_test().get("user_id")
    except Exception as e:
        log.warning("slack_auth_test_failed", error=str(e))

    threading.Thread(target=_start_relay_thread, daemon=True).start()
    threading.Thread(target=_start_control_server, daemon=True).start()
    threading.Thread(target=_watchdog, daemon=True).start()
    log.info("slack_registerabot_adapter_starting", service=SERVICE_SLUG,
             control_port=CONTROL_PORT, watchdog_grace=WATCHDOG_GRACE)
    _socket_handler = SocketModeHandler(slack_app, SLACK_APP_TOKEN)
    _socket_handler.start()  # blocks


if __name__ == "__main__":
    main()
