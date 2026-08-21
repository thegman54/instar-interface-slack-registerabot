# instar-interface-slack-registerabot

Thin **Slack → RegisterABot** adapter. Not a full connector — it bridges Slack to the bot
over the RegisterABot relay as a *service client*, using its own relay identity
(`SLACK_REGISTERABOT_TOKEN`). It never talks to the instar gatekeeper/bot directly.

Flow: Slack message (+ files) via Socket Mode → download files with the Slack token →
`chat_request` envelope `{messages, user_context, attachments}` → relay `/ws/service/{slug}?key=…&bot=…`
→ bot → `final_response` (matched by `session_id`) → posted back to the Slack thread.

Files ride inline (base64) in the payload; the bot-side registerabot connector extracts them
and runs the `on_file` barrier (parse → stage → offer). R2 for oversized files is a relay-side
enhancement (see project-instar docs/INTERFACE_BUS.md).

## Outbound — starting a conversation

Everything above is reactive: the bot can only speak inside a session an inbound Slack
message created. `POST /outbound` on the control plane (`:8092`) is the other direction.

```
POST http://slack-registerabot:8092/outbound
{ "to": "U0123ABC" | "ross@example.com" | "ross" | "C0123CHAN",
  "text": "My human, Alex Glickman, asked me to pass this along…",
  "join_url": "https://…",        # optional — adds a native Slack call block
  "title": "Sync with Seven",     # optional — call block title
  "bot": "seven" }                # optional — 409s if it isn't the connected bot
```

- **Cold DMs work.** `conversations.open(users=U…)` creates the DM with someone the bot has
  never spoken to. Requires `im:write` + `chat:write`; name/email resolution additionally
  needs `users:read` / `users:read.email`.
- **Reachable only from `instar-internal`** — the same trust boundary as
  `/slugs/{bot}/connect`. The bot is not on that network; it reaches this through
  mcp-server → tool-executor (the `slack_outbound` skill).
- The response's `session_id` is keyed exactly as an inbound top-level message from that
  user would be, so **their reply continues this conversation** rather than forking one.

### Calls, and why not huddles

Slack has **no API to start or join a huddle** — no `huddles.*` methods, no bot scope; they
are client-only. `join_url` therefore points at our own media path (the registerabot WebRTC
gateway), and `calls.add` just renders a native block for it. `calls.add` rings nobody: the
message is the entire notification. Without `calls:write` the endpoint degrades to posting a
plain link rather than failing.

**Known limitation:** an outbound DM seeds the routing entry but does not prime the bot's
side with what it just said, so when the recipient replies the bot opens that conversation
without the outbound text in its history. Put the necessary context in `text`.

## Setup (config, not code)
1. In the RegisterABot portal/Supabase: create a **service** (slug + API key) and **authorize**
   it for the target bot. The API key becomes `SLACK_REGISTERABOT_TOKEN`.
2. Put all creds in Infisical (see manifest.yaml).
3. Deploy the stack; it connects Socket Mode + the relay WS. Outbound-only, no ports.
