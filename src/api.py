"""Slack → RegisterABot adapter.

A THIN bridge, not a full connector. It receives Slack messages (and files) via Socket
Mode and forwards them to the bot over the RegisterABot relay as a *service client* — using
this adapter's OWN relay identity (SLACK_REGISTERABOT_TOKEN). It never talks to the instar
gatekeeper/bot directly; the relay routes to the bot, and the bot-side registerabot connector
extracts the attachments and runs the on_file barrier.

Two creds (both injected from Infisical by the stack):
  - Slack: SLACK_BOT_TOKEN + SLACK_APP_TOKEN — to receive events, download dropped files
    (Slack files sit behind url_private; only the Slack token can fetch them), and upload the
    bot's outbound files/audio/video back into the thread.
  - Relay: SLACK_REGISTERABOT_TOKEN — this adapter's identity ON the relay.

Protocol (registerabot SDK): connect wss://relay/ws/service/{serviceSlug}?key=…&bot=…,
send {type:"chat_request", session_id, from:{kind:service}, to:{kind:bot}, payload:
JSON({messages:[{role,content,attachments}], user_context})}. The relay routes EVERY bot→
service frame (final_response, audio, video, idle) back by session_id, so we keep a
session_id → {channel, thread_ts} map and dispatch each frame to the right thread:
  - final_response  → post reply text, upload any `attachments` + embedded `audio`
  - video           → upload the talking-head mp4 (audio baked in; plays inline in Slack)
  - audio           → ignored (already embedded in final_response; avoids double audio)
  - idle            → ignored (a browser-only looping avatar; meaningless in Slack)
"""

import asyncio
import base64
import json
import os
import threading
import time
import uuid

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
# Service + bot slugs differ per adapter, so they're SLACK_-prefixed (like the token) to avoid
# colliding with the Zoom adapter's values in a shared Infisical project. Generic fallbacks kept.
SERVICE_SLUG = (os.environ.get("SLACK_REGISTERABOT_SERVICE_SLUG")
                or os.environ.get("REGISTERABOT_SERVICE_SLUG", "slack-adapter"))  # who we speak AS
SERVICE_KEY = os.environ.get("SLACK_REGISTERABOT_TOKEN", "")           # that service's key
BOT_SLUG = (os.environ.get("SLACK_REGISTERABOT_BOT_SLUG")
            or os.environ.get("REGISTERABOT_BOT_SLUG", ""))            # which bot we route TO
SESSION_TTL = int(os.environ.get("SESSION_TTL", "600"))   # keep dest mapping for trailing frames

# --- relay websocket state (one persistent service connection) ----------------
_relay_ws = None
_relay_loop: asyncio.AbstractEventLoop | None = None
_bot_user_id: str | None = None
# session_id -> {"channel", "thread_ts", "ts"} — where to post this turn's frames
_sessions: dict[str, dict] = {}


def _prune_sessions():
    cutoff = time.time() - SESSION_TTL
    for sid in [s for s, v in _sessions.items() if v.get("ts", 0) < cutoff]:
        _sessions.pop(sid, None)


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
    """Get the bytes for a media item, whichever form the relay delivered:
      - inline base64 (`data`/`base64`/`audio_base64`) — small items the relay kept inline
      - a hosted ref (`url`) — the relay stored it (TTL ~10 min); fetch to materialize
    Slack wants the actual file (for an inline player), so we pull the bytes either way."""
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
    sid = env.get("session_id")
    dest = _sessions.get(sid)
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
        # outbound files the bot attached (e.g. the exported CSV)
        for a in payload.get("attachments") or []:
            if a.get("type") == "audio":
                continue
            data = await _resolve_bytes(a)
            if data:
                name = a.get("name", "file")
                await loop.run_in_executor(None, _upload_file, channel, thread, data, name, name)
        # embedded voice audio (mp3) — only present when no talking-head video for this turn
        audio = payload.get("audio") or {}
        if audio:
            adata = await _resolve_bytes(audio)
            if adata:
                await loop.run_in_executor(None, _upload_file, channel, thread, adata,
                                           "voice.mp3", "Voice")

    elif ftype == "video":
        # {format:'mp4', url|base64} — upload the talking-head clip; Slack plays it inline.
        vdata = await _resolve_bytes(payload)
        if vdata:
            await loop.run_in_executor(None, _upload_file, channel, thread, vdata,
                                       "avatar.mp4", "Avatar")
    # 'audio' (duplicate of embedded) and 'idle' frames are intentionally ignored.


async def _relay_client():
    """Maintain one persistent service-client WS to the relay; dispatch every bot frame."""
    global _relay_ws
    url = f"{RELAY_URL}/ws/service/{SERVICE_SLUG}?key={SERVICE_KEY}&bot={BOT_SLUG}"
    while True:
        try:
            async with websockets.connect(url, max_size=None) as ws:
                _relay_ws = ws
                log.info("relay_connected", service=SERVICE_SLUG, bot=BOT_SLUG)
                async for raw in ws:
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
        await asyncio.sleep(5)  # reconnect


async def _send_to_bot(sid: str, text: str, user_id: str, attachments: list):
    """Send one chat_request over the relay. The reply arrives asynchronously as frames."""
    if _relay_ws is None:
        return
    envelope = {
        "v": 1, "type": "chat_request", "session_id": sid,
        "timestamp": int(time.time() * 1000),
        "from": {"kind": "service", "slug": SERVICE_SLUG, "name": "Slack"},
        "to": {"kind": "bot", "slug": BOT_SLUG},
        "encrypted": False,
        # Attachments go ON THE MESSAGE — the bot-side registerabot connector reads
        # m.get('attachments') per message and forwards them to /process, which feeds
        # the on_file barrier. (Top-level payload attachments would be missed.)
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
    """Download Slack files (url_private needs the Slack bot token) → attachment dicts
    {name, mime, encoding:base64, data}. This is the one thing only the adapter can do."""
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


# --- Slack Socket Mode ---------------------------------------------------------
slack_app = App(token=SLACK_BOT_TOKEN)


@slack_app.event("message")
def on_slack_message(event, say):
    # ignore the bot's own + edits/deletes (but NOT file_share — that's a real upload)
    if event.get("bot_id") or event.get("user") == _bot_user_id:
        return
    if event.get("subtype") in ("message_changed", "message_deleted", "channel_join"):
        return

    text = (event.get("text") or "").strip()
    files = event.get("files", [])
    if not text and not files:
        return

    attachments = _fetch_slack_files(files)  # sync, with the Slack token
    user_id = event.get("user", "slack")
    channel = event.get("channel", "")
    thread_ts = event.get("thread_ts") or event.get("ts")

    if _relay_loop is None or _relay_ws is None:
        say(text="I couldn't reach the bot right now — try again in a moment.",
            thread_ts=thread_ts)
        return

    # Register where this turn's frames should land, then fire the request (non-blocking).
    sid = str(uuid.uuid4())
    _prune_sessions()
    _sessions[sid] = {"channel": channel, "thread_ts": thread_ts, "ts": time.time()}
    asyncio.run_coroutine_threadsafe(
        _send_to_bot(sid, text or "(file attached)", user_id, attachments), _relay_loop)


def _start_relay_thread():
    """Run the relay asyncio client on its own thread + loop."""
    global _relay_loop
    _relay_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(_relay_loop)
    _relay_loop.run_until_complete(_relay_client())


def main():
    global _bot_user_id
    missing = [k for k, v in {
        "SLACK_BOT_TOKEN": SLACK_BOT_TOKEN, "SLACK_APP_TOKEN": SLACK_APP_TOKEN,
        "REGISTERABOT_RELAY_URL": RELAY_URL, "SLACK_REGISTERABOT_TOKEN": SERVICE_KEY,
        "REGISTERABOT_BOT_SLUG": BOT_SLUG}.items() if not v]
    if missing:
        raise SystemExit(f"Missing required env: {missing}")

    try:
        _bot_user_id = slack_app.client.auth_test().get("user_id")
    except Exception as e:
        log.warning("slack_auth_test_failed", error=str(e))

    threading.Thread(target=_start_relay_thread, daemon=True).start()
    log.info("slack_registerabot_adapter_starting", service=SERVICE_SLUG, bot=BOT_SLUG)
    SocketModeHandler(slack_app, SLACK_APP_TOKEN).start()  # blocks


if __name__ == "__main__":
    main()
