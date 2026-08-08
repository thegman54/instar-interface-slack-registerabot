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
import time
from http.server import BaseHTTPRequestHandler, HTTPServer

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

# --- relay + routing state ----------------------------------------------------
_relay_ws = None
_relay_loop: asyncio.AbstractEventLoop | None = None
_bot_user_id: str | None = None
_active_bot: str | None = None            # which bot we route TO — comes from the profile
# session_id -> {"channel", "thread_ts", "ts"} — where to post this turn's frames
_sessions: dict[str, dict] = {}


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
            await loop.run_in_executor(None, _upload_file, channel, thread, vdata,
                                       "avatar.mp4", "Avatar")
    # 'audio' (duplicate of embedded) and 'idle' frames are intentionally ignored.


async def _relay_client():
    """Maintain the service-client WS to the relay for whichever bot the profile connected us
    to. We authenticate with OUR OWN service slug + key; the bot slug is the profile's.
    Reconnects when _active_bot changes (connect/disconnect close the socket)."""
    global _relay_ws
    while True:
        bot = _active_bot
        if not bot:
            await asyncio.sleep(1)   # idle until a profile connects us
            continue
        url = f"{RELAY_URL}/ws/service/{SERVICE_SLUG}?key={SERVICE_KEY}&bot={bot}"
        try:
            async with websockets.connect(url, max_size=None) as ws:
                _relay_ws = ws
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
        await asyncio.sleep(2)


async def _send_to_bot(sid: str, text: str, user_id: str, attachments: list):
    """Send one chat_request over the relay to the currently-connected bot."""
    if _relay_ws is None or not _active_bot:
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
    except Exception as e:
        log.warning("send_to_bot_failed", error=str(e))


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
            return self._reply(200, {"status": "ok", "service": SERVICE_SLUG,
                                     "active_bot": _active_bot})
        self._reply(404, {"error": "not found"})

    def do_POST(self):
        # The profile hands us ONLY its bot slug (in the path). We authenticate as our own
        # service; the profile's own token is the bot's, not ours, so we ignore any body.
        parts = [p for p in self.path.split("/") if p]  # ['slugs', '{bot}', 'connect']
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
    HTTPServer(("0.0.0.0", CONTROL_PORT), _Control).serve_forever()


# --- Slack Socket Mode ---------------------------------------------------------
slack_app = App(token=SLACK_BOT_TOKEN)


@slack_app.event("message")
def on_slack_message(event, say):
    if event.get("bot_id") or event.get("user") == _bot_user_id:
        return
    if event.get("subtype") in ("message_changed", "message_deleted", "channel_join"):
        return

    text = (event.get("text") or "").strip()
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
    asyncio.run_coroutine_threadsafe(
        _send_to_bot(sid, text or "(file attached)", user_id, attachments), _relay_loop)


def _start_relay_thread():
    global _relay_loop
    _relay_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(_relay_loop)
    _relay_loop.run_until_complete(_relay_client())


def main():
    global _bot_user_id
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
    log.info("slack_registerabot_adapter_starting", service=SERVICE_SLUG, control_port=CONTROL_PORT)
    SocketModeHandler(slack_app, SLACK_APP_TOKEN).start()  # blocks


if __name__ == "__main__":
    main()
