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

## Setup (config, not code)
1. In the RegisterABot portal/Supabase: create a **service** (slug + API key) and **authorize**
   it for the target bot. The API key becomes `SLACK_REGISTERABOT_TOKEN`.
2. Put all creds in Infisical (see manifest.yaml).
3. Deploy the stack; it connects Socket Mode + the relay WS. Outbound-only, no ports.
