"""Fetch specific secrets from Infisical and print as KEY=VALUE lines."""
import json, os, sys, urllib.request

url = os.environ.get("INFISICAL_URL", "")
token = os.environ.get("INFISICAL_TOKEN", "")
project_id = os.environ.get("INFISICAL_PROJECT_ID", "")
env = os.environ.get("INFISICAL_ENVIRONMENT", "prod")
wanted = set(sys.argv[1:])

if not (url and token and project_id and wanted):
    sys.exit(0)

try:
    req = urllib.request.Request(
        f"{url}/api/v3/secrets/raw?workspaceId={project_id}&environment={env}",
        headers={"Authorization": f"Bearer {token}"},
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        data = json.loads(resp.read())
    for s in data.get("secrets", []):
        if s["secretKey"] in wanted:
            key = s["secretKey"]
            val = s["secretValue"]
            print(f"{key}={val}")
except Exception as e:
    print(f"# Error: {e}", file=sys.stderr)
