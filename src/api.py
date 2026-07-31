"""Slack → RegisterABot adapter.

A THIN bridge, not a full connector. It receives Slack messages (and files) via Socket
Mode and forwards them to the bot over the RegisterABot relay as a *service client* — using
this adapter's OWN relay identity (SLACK_REGISTERABOT_TOKEN). It never talks to the instar
gatekeeper/bot directly; the relay routes to the bot, and the bot-side registerabot connector
extracts the attachments and runs the on_file barrier.

Two creds (both injected from Infisical by the stack):
  - Slack: SLACK_BOT_TOKEN + SLACK_APP_TOKEN — to receive events and to download dropped
    files (Slack files sit behind url_private; only the Slack token can fetch them).
  - Relay: SLACK_REGISTERABOT_TOKEN — this adapter's identity ON the relay.

Protocol (registerabot SDK): connect wss://relay/ws/service/{serviceSlug}?key=…&bot=…,
send {type:"chat_request", session_id, from:{kind:service}, to:{kind:bot}, payload:
JSON({messages:[{role,content}], user_context, attachments})}, receive {type:"final_response",
session_id, payload:JSON({reply})}. Correlate request↔reply by session_id.
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
RELAY_URL = os.environ.get("REGISTERABOT_RELAY_URL", "").rstrip("/")   # wss://relay…
SERVICE_SLUG = os.environ.get("REGISTERABOT_SERVICE_SLUG", "slack-adapter")
SERVICE_KEY = os.environ.get("SLACK_REGISTERABOT_TOKEN", "")           # relay identity
BOT_SLUG = os.environ.get("REGISTERABOT_BOT_SLUG", "")
REPLY_TIMEOUT = int(os.environ.get("REPLY_TIMEOUT", "300"))

# --- relay websocket state (one persistent service connection) ----------------
_relay_ws = None
_relay_loop: asyncio.AbstractEventLoop | None = None
_pending: dict[str, asyncio.Future] = {}   # session_id -> future awaiting final_response
_bot_user_id: str | None = None


async def _relay_client():
    """Maintain one persistent service-client WS to the relay; resolve replies by session_id."""
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
                    if env.get("type") == "final_response":
                        sid = env.get("session_id")
                        fut = _pending.pop(sid, None)
                        if fut and not fut.done():
                            try:
                                fut.set_result(json.loads(env.get("payload") or "{}"))
                            except Exception:
                                fut.set_result({})
        except Exception as e:
            log.warning("relay_disconnected", error=str(e))
        finally:
            _relay_ws = None
        await asyncio.sleep(5)  # reconnect


async def _send_to_bot(text: str, user_id: str, attachments: list) -> dict | None:
    """Send one chat_request over the relay and await the bot's final_response."""
    if _relay_ws is None:
        return None
    sid = str(uuid.uuid4())
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
    fut = _relay_loop.create_future()
    _pending[sid] = fut
    try:
        await _relay_ws.send(json.dumps(envelope))
        return await asyncio.wait_for(fut, timeout=REPLY_TIMEOUT)
    except Exception as e:
        _pending.pop(sid, None)
        log.warning("send_to_bot_failed", error=str(e))
        return None


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

    # forward over the relay (on its loop) and wait for the reply
    result = None
    if _relay_loop is not None:
        try:
            future = asyncio.run_coroutine_threadsafe(
                _send_to_bot(text or "(file attached)", user_id, attachments), _relay_loop)
            result = future.result(timeout=REPLY_TIMEOUT + 10)
        except Exception as e:
            log.warning("relay_roundtrip_failed", error=str(e))

    reply = (result or {}).get("reply") if result else None
    say(text=reply or "I couldn't reach the bot right now — try again in a moment.",
        thread_ts=event.get("thread_ts") or event.get("ts"))


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
