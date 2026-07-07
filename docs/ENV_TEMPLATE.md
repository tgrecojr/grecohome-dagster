# Environment template

Canonical contents for the repo-root `.env.example` (your security hooks block the
agent from writing any `.env.*` path, so create it yourself — see "Bootstrapping
`.env.example`" below). In production these are injected by Ansible from a secrets
manager. **Never commit a real `.env`.**

| Variable | Required | Default | Purpose |
|---|---|---|---|
| `BRONZE_ROOT` | yes | — | Root dir for bronze raw capture (mounted volume / object-store path) |
| `BRONZE_MONITOR_DIR` | no | — | Writable dir for bronze-**check** state (schema-drift baselines), kept **outside** `BRONZE_ROOT`. Unset → schema-drift check no-ops. See [VALIDATION.md](VALIDATION.md) |
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

### Garmin (`grecohome-garmin`) — its own container

`BRONZE_ROOT`/`LOG_LEVEL`/`ENVIRONMENT` and the Dagster-instance vars below apply here too.

| Variable | Required | Default | Purpose |
|---|---|---|---|
| `GARMINTOKENS` | no | `/secrets/garmin` | Mounted, writable dir for the garminconnect token store (separate from bronze) |
| `GARMINCONNECT_EMAIL` | yes | — | Garmin Connect login email |
| `GARMINCONNECT_BASE64_PASSWORD` | yes | — | Base64-encoded Garmin password |
| `GARMINCONNECT_IS_CN` | no | `false` | Use the Garmin China domain |
| `LOOKBACK_DAYS` | no | `7` | Trailing completed partitions the daily schedule captures (once each) |
| `FETCH_SELECTION` | no | (empty = all) | Allowlist of catalog collection names |
| `FETCH_EXCLUDE` | no | (empty) | Denylist (wins over selection); empty = capture everything |
| `RATE_LIMIT_SECONDS` | no | `2.0` | Sleep between API calls within a run |
| `WEEKLY_WEEKS` | no | `4` | Trailing weeks for the weekly-aggregate endpoints |
| `CAPTURE_ALT_FORMATS` | no | `false` | Also capture TCX/GPX/KML/CSV activity exports |
| `PROCESSOR_VERSION` | no | `dev` | Stamped into bronze sidecars |

### Lingo (`grecohome-lingo`) — its own container

`BRONZE_ROOT`/`LOG_LEVEL`/`ENVIRONMENT` and the Dagster-instance vars below apply here too.

| Variable | Required | Default | Purpose |
|---|---|---|---|
| `GDRIVE_SERVICE_ACCOUNT_PATH` | yes | — | Mounted, read-only path to the Google service-account key JSON |
| `GDRIVE_FOLDER_ID` | yes | — | Drive folder id the sensor watches (the folder shared with the SA) |
| `GDRIVE_POLL_INTERVAL_MINUTES` | no | `5` | Sensor minimum interval between folder listings |

> The SA key is the only credential; it's mounted, never baked into the image and never written to
> bronze. See [DEPLOYMENT — GCP service-account setup](DEPLOYMENT.md#gcp-service-account-setup).

### Soil / NOAA USCRN (`grecohome-soil`) — its own container

`BRONZE_ROOT`/`LOG_LEVEL`/`ENVIRONMENT` and the Dagster-instance vars below apply here too.
**No secrets** — the source is public NOAA HTTP, so there is nothing to mount beyond bronze.

| Variable | Required | Default | Purpose |
|---|---|---|---|
| `USCRN_STATION` | no | `PA_Avondale_2_N` | Station filename stem (`STATE_LOCATION_DIST_DIR`) |
| `USCRN_BASE_URL` | no | `.../products/hourly02` | USCRN hourly02 product base URL |
| `USCRN_LOOKBACK_DAYS` | no | `2` | Trailing daily partitions the 6-hourly schedule re-captures |
| `USCRN_START_DATE` | no | `2010-01-01` | Backfill floor for the daily partition set |

### Location (`grecohome-location`) — its own container

Promotes the `locationrelay` service's raw staging files into bronze. **No source-API calls, no
secret.** `BRONZE_ROOT`/`LOG_LEVEL`/`ENVIRONMENT` and the Dagster-instance vars below apply too.
**Run this container as uid 1000 at runtime** (image builds as `nonroot` like every other subject;
set the runtime user, e.g. compose `user: "1000:998"`) and mount `RELAY_CAPTURE_DIR` **read-only**
(staging files are `0600` owned by uid 1000; only uid 1000 can read them).

| Variable | Required | Default | Purpose |
|---|---|---|---|
| `RELAY_CAPTURE_DIR` | yes | — | Relay data dir (host `/opt/docker/locationrelay/data`); mount **read-only**. Promoter never writes under it |
| `LOCATION_STATE_DIR` | yes | — | Writable dir for the per-stream promoted-set; **outside** `BRONZE_ROOT` (refused otherwise) |
| `LOCATION_PROMOTE_WINDOW_DAYS` | no | `3` | Trailing staging window the promoter scans (keep > promoter downtime, < relay retention) |
| `LOCATION_PROMOTE_LAG_HOURS` | no | `6` | Promote-lag ERROR: un-promoted staging file older than this fails |
| `LOCATION_FRESHNESS_WARN_HOURS` | no | `24` | Receipt-freshness WARN tolerance (hours since newest received POST) |
| `LOCATION_FRESHNESS_ERROR_HOURS` | no | `168` | Receipt-freshness ERROR tolerance (very long gap) |
| `LOCATION_RECENT_PARTITIONS` | no | `14` | Trailing bronze partitions the checks inspect |

> The auth token is header/query-only at the relay and never touches a staging body, so nothing
> secret reaches bronze. See [packages/location/docs/LOCATION.md](../packages/location/docs/LOCATION.md).

### Silver (`grecohome-silver`) — its own container, cross-subject

Silver reads bronze and writes Parquet; it makes **no source-API calls** (no secrets, no
token/key mount). `LOG_LEVEL`/`ENVIRONMENT` and the Dagster-instance vars below apply here
too. Mount `BRONZE_ROOT` **read-only** and `SILVER_ROOT` writable on a separate volume.

| Variable | Required | Default | Purpose |
|---|---|---|---|
| `BRONZE_ROOT` | yes | — | Bronze tree to read (mount **read-only**; silver never writes under it) |
| `SILVER_ROOT` | yes | — | Writable root for silver Parquet, **outside** `BRONZE_ROOT` (writes there are refused) |
| `SILVER_MONITOR_DIR` | no | — | Reserved for the future silver monitor (mirrors `BRONZE_MONITOR_DIR`); kept **outside** `SILVER_ROOT`. Unused today; unset → future silver checks no-op |
| `USCRN_TIMEZONE` | no | `America/New_York` | IANA tz of the USCRN station; derives `silver_weather`'s **local** observation day (DST-aware). Single-station. |

### Gold (`grecohome-gold`) — its own container, cross-layer

Gold reads silver and writes marts; **no source-API calls** (no secrets). `LOG_LEVEL`/
`ENVIRONMENT` and the Dagster-instance vars below apply too. Mount `SILVER_ROOT` **read-only**
and `GOLD_ROOT` writable on a separate volume.

| Variable | Required | Default | Purpose |
|---|---|---|---|
| `SILVER_ROOT` | yes | — | Silver tree to read (mount **read-only**; gold never writes under it) |
| `GOLD_ROOT` | yes | — | Writable root for gold marts, **outside** `SILVER_ROOT` (writes there are refused) |
| `GOLD_MONITOR_DIR` | no | — | Reserved for a future gold monitor; kept **outside** `GOLD_ROOT`. Unused today |

> **Grafana dashboards** read the lake directly via a DuckDB datasource plugin over the
> read-only `/data/{bronze,silver,gold}` mounts — no env vars, no separate container. (The
> earlier `grecohome-lakequery` HTTP service was retired; see [DEPLOYMENT.md](DEPLOYMENT.md#grafana-data-lake-dashboards).)

### Dagster instance (required at deploy, not for local `dagster dev`)

The host uses **`DefaultRunLauncher`**, so each run executes as a subprocess **inside the
code-location container** and writes to the host instance's Postgres event/run/schedule
storage. The container therefore needs `DAGSTER_HOME` pointing at the *same* `dagster.yaml`
the daemon uses (mounted, not baked) plus the Postgres connection vars that file references.
`dagster-postgres` is already bundled in the image. These are **not** read by the app's
settings — Dagster reads them.

| Variable | Required | Purpose |
|---|---|---|
| `DAGSTER_HOME` | yes (deploy) | Dir holding the mounted `dagster.yaml` (matches the daemon's) |
| `DAGSTER_POSTGRES_USER` | yes (deploy) | Instance Postgres user (referenced by `dagster.yaml`) |
| `DAGSTER_POSTGRES_PASSWORD` | yes (deploy) | Instance Postgres password |
| `DAGSTER_POSTGRES_HOST` | yes (deploy) | Instance Postgres host |
| `DAGSTER_POSTGRES_DB` | yes (deploy) | Instance Postgres database |

> Note: this is Dagster's *own* metadata DB (runs/events/schedules) — separate from, and not a
> contradiction of, the app having no database. Bronze is files and silver/gold are Parquet; the
> app writes no business data to Postgres.

## Bootstrapping `.env.example`

```bash
cat > .env.example <<'EOF'
# grecohome-dagster — example environment for the WHOOP code location.
# Copy to .env for local dev. Production injects these via Ansible/secrets manager.

# --- Shared (grecohome-core BaseSubjectSettings) ---
BRONZE_ROOT=/data/bronze
# Bronze-check state (schema-drift baselines); MUST be outside BRONZE_ROOT.
BRONZE_MONITOR_DIR=/data/bronze-monitor
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

# --- Silver (grecohome-silver); cross-subject layer, no source API/secrets ---
# Mount BRONZE_ROOT read-only here; SILVER_ROOT must be a separate path outside it.
SILVER_ROOT=/data/silver
# Reserved for the future silver monitor (unused today); MUST be outside SILVER_ROOT.
SILVER_MONITOR_DIR=/data/silver-monitor
# IANA tz of the USCRN station for silver_weather's local day (optional; DST-aware).
USCRN_TIMEZONE=America/New_York

# --- Gold (grecohome-gold); analysis marts from silver, no source API/secrets ---
# Mount SILVER_ROOT read-only here; GOLD_ROOT must be a separate path outside it.
GOLD_ROOT=/data/gold
# Reserved for the future gold monitor (unused today); MUST be outside GOLD_ROOT.
GOLD_MONITOR_DIR=/data/gold-monitor

# --- Dagster instance (deploy only; not needed for local `dagster dev`) ---
# Required because DefaultRunLauncher executes runs inside this container, which
# then writes to the host instance's Postgres. Mount the daemon's dagster.yaml
# and point DAGSTER_HOME at it.
DAGSTER_HOME=/opt/dagster/dagster_home
DAGSTER_POSTGRES_USER=dagster
DAGSTER_POSTGRES_PASSWORD=your_dagster_pg_password
DAGSTER_POSTGRES_HOST=your_dagster_pg_host
DAGSTER_POSTGRES_DB=dagster
EOF
```
