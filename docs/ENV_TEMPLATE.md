# Environment template

Canonical contents for the repo-root `.env.example` (your security hooks block the
agent from writing any `.env.*` path, so create it yourself — see "Bootstrapping
`.env.example`" below). In production these are injected by Ansible from a secrets
manager. **Never commit a real `.env`.**

| Variable | Required | Default | Purpose |
|---|---|---|---|
| `BRONZE_ROOT` | yes | — | Root dir for bronze raw capture (mounted volume / object-store path) |
| `LOG_LEVEL` | no | `INFO` | Log level |
| `ENVIRONMENT` | no | `development` | Environment name |
| `WHOOP_CLIENT_ID` | yes | — | Whoop OAuth client id |
| `WHOOP_CLIENT_SECRET` | yes | — | Whoop OAuth client secret |
| `WHOOP_REDIRECT_URI` | no | `http://localhost:8000/callback` | OAuth callback |
| `WHOOP_API_BASE_URL` | no | `https://api.prod.whoop.com` | API base |
| `WHOOP_AUTH_URL` | no | `.../oauth/oauth2/auth` | OAuth authorize endpoint |
| `WHOOP_TOKEN_URL` | no | `.../oauth/oauth2/token` | OAuth token endpoint |
| `WHOOP_TOKEN_PATH` | yes | — | Mounted, writable path to the OAuth token JSON file |
| `MAX_REQUESTS_PER_MINUTE` | no | `60` | Whoop API rate cap |
| `RECONCILE_WINDOW_DAYS` | no | `7` | Trailing reconcile overlap; schedule re-captures this + 1 partition |

## Bootstrapping `.env.example`

```bash
cat > .env.example <<'EOF'
# grecohome-dagster — example environment for the WHOOP code location.
# Copy to .env for local dev. Production injects these via Ansible/secrets manager.

# --- Shared (grecohome-core BaseSubjectSettings) ---
BRONZE_ROOT=/data/bronze
LOG_LEVEL=INFO
ENVIRONMENT=development

# --- Whoop OAuth (grecohome-whoop) ---
WHOOP_CLIENT_ID=your_whoop_client_id
WHOOP_CLIENT_SECRET=your_whoop_client_secret
WHOOP_REDIRECT_URI=http://localhost:8000/callback
WHOOP_API_BASE_URL=https://api.prod.whoop.com
WHOOP_AUTH_URL=https://api.prod.whoop.com/oauth/oauth2/auth
WHOOP_TOKEN_URL=https://api.prod.whoop.com/oauth/oauth2/token
WHOOP_TOKEN_PATH=/secrets/whoop/token.json

# --- Whoop tuning ---
MAX_REQUESTS_PER_MINUTE=60
RECONCILE_WINDOW_DAYS=7
EOF
```
