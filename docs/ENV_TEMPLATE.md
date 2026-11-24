# Environment template

Canonical contents for the repo-root `.env.example` (your security hooks block the
agent from writing any `.env.*` path, so create it yourself — see "Bootstrapping
`.env.example`" below). In production these are injected by Ansible from a secrets
manager. **Never commit a real `.env`.**

| Variable | Required | Default | Purpose |
|---|---|---|---|
| `BRONZE_ROOT` | yes | — | Root dir for bronze raw capture (mounted volume / object-store path) |
| `BRONZE_MONITOR_DIR` | no | — | Writable dir for bronze-**check** state (schema-drift baselines), kept **outside** `BRONZE_ROOT`. Unset → schema-drift check no-ops. See [bronze checks](intent/core/checks/checks-design.md) |
| `LOG_LEVEL` | no | `INFO` | Log level |
| `ENVIRONMENT` | no | `development` | Environment name |

These shared vars come from `grecohome-core`'s `BaseSubjectSettings` and apply to every
container that reads bronze (soil and silver); gold reads `LOG_LEVEL`/`ENVIRONMENT` only. Whoop
and Garmin credentials belong to [WearStream](https://github.com/chainringlabs/wearstream)'s own
deployment, not to any container here.

### Soil / NOAA USCRN (`grecohome-soil`) — its own container

The shared vars above and the Dagster-instance vars below apply here too.
**No secrets** — the source is public NOAA HTTP, so there is nothing to mount beyond bronze.

| Variable | Required | Default | Purpose |
|---|---|---|---|
| `USCRN_STATION` | no | `PA_Avondale_2_N` | Station filename stem (`STATE_LOCATION_DIST_DIR`) |
| `USCRN_BASE_URL` | no | `.../products/hourly02` | USCRN hourly02 product base URL |
| `USCRN_LOOKBACK_DAYS` | no | `2` | Trailing daily partitions the 6-hourly schedule re-captures |
| `USCRN_CORRECTION_LOOKBACK_DAYS` | no | `400` | Trailing partitions the **weekly** correction schedule re-slices in one run, picking up NOAA's late in-place corrections of the year files (gap hours recovered from the logger, corrupt rows repaired). 400 covers the current + previous year file; older corrections need a manual backfill |
| `USCRN_START_DATE` | no | `2010-01-01` | Backfill floor for the daily partition set |

### Silver (`grecohome-silver`) — its own container, cross-subject

Silver reads bronze and writes Parquet; it makes **no source-API calls** (no secrets, no
key mount). `LOG_LEVEL`/`ENVIRONMENT` and the Dagster-instance vars below apply here
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
| `GOLD_WEATHER_MIN_VALID_HOURS` | no | `22` | `gold_daily_weather` completeness gate: valid hours a field needs in a local day before its daily aggregate is reported (else NULL, with the `*_hours` count kept). Mirrors NCEI's daily product |

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
# grecohome-dagster — example environment for the SOIL code location, plus the
# silver, gold and Dagster-instance sections (each container reads only its own vars).
# Copy to .env for local dev. Production injects these via Ansible/secrets manager.

# --- Shared (grecohome-core BaseSubjectSettings) ---
BRONZE_ROOT=/data/bronze
# Bronze-check state (schema-drift baselines); MUST be outside BRONZE_ROOT.
BRONZE_MONITOR_DIR=/data/bronze-monitor
LOG_LEVEL=INFO
ENVIRONMENT=development

# --- Soil / NOAA USCRN (grecohome-soil); public source, no secrets ---
USCRN_STATION=PA_Avondale_2_N
USCRN_BASE_URL=https://www.ncei.noaa.gov/pub/data/uscrn/products/hourly02
USCRN_LOOKBACK_DAYS=2
USCRN_CORRECTION_LOOKBACK_DAYS=400
USCRN_START_DATE=2010-01-01

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
# gold_daily_weather completeness gate: valid hours per local day (optional).
GOLD_WEATHER_MIN_VALID_HOURS=22

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
