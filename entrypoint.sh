#!/bin/sh
# Fetch this adapter's OWN secrets from Infisical at startup, export them, then exec the app.
# These are the adapter's identity (its service slug + service key) + Slack platform creds —
# NOT the bot's key (that lives in the profile and is the registerabot connector's concern).

if [ -n "$INFISICAL_URL" ] && [ -n "$INFISICAL_TOKEN" ] && [ -n "$INFISICAL_PROJECT_ID" ]; then
    echo "Fetching secrets from Infisical..."
    python3 /app/fetch_secrets.py \
        SLACK_BOT_TOKEN SLACK_APP_TOKEN REGISTERABOT_RELAY_URL \
        SLACK_REGISTERABOT_SERVICE_SLUG SLACK_REGISTERABOT_TOKEN \
        > /tmp/.secrets 2>/tmp/.secrets_err

    if [ -s /tmp/.secrets ]; then
        while IFS= read -r line; do
            export "$line"
            echo "  Loaded ${line%%=*}"
        done < /tmp/.secrets
        rm -f /tmp/.secrets
    else
        echo "WARNING: No secrets returned from Infisical"
        cat /tmp/.secrets_err 2>/dev/null
    fi
    rm -f /tmp/.secrets_err
else
    echo "No Infisical config — using environment variables as-is"
fi

exec python -m src.api
