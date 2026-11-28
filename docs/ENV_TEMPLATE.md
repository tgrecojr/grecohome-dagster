# Environment template

Canonical contents for the repo-root `.env.example` (your security hooks block the
agent from writing any `.env.*` path, so create it yourself — see "Bootstrapping
`.env.example`" below). In production these are injected by Ansible from a secrets
manager. **Never commit a real `.env`.**

| Variable | Required | Default | Purpose |
|---|---|---|---|
| `BRONZE_ROOT` | yes | — | Root of the bronze tree WearStream writes (mounted volume / object-store path); read-only here |
| `LOG_LEVEL` | no | `INFO` | Log level |
| `ENVIRONMENT` | no | `development` | Environment name |

These shared vars come from `grecohome-core`'s `BaseSubjectSettings` and apply to every
container that reads bronze (silver); gold reads `LOG_LEVEL`/`ENVIRONMENT` only. Whoop
and Garmin credentials belong to [WearStream](https://github.com/chainringlabs/wearstream)'s own
deployment, not to any container here.

### Silver (`grecohome-silver`) — its own container, cross-subject

Silver reads bronze and writes Parquet; it makes **no source-API calls** (no secrets, no
key mount). `LOG_LEVEL`/`ENVIRONMENT` and the Dagster-instance vars below apply here
too. Mount `BRONZE_ROOT` **read-only** and `SILVER_ROOT` writable on a separate volume.

| Variable | Required | Default | Purpose |
|---|---|---|---|
| `BRONZE_ROOT` | yes | — | Bronze tree to read (mount **read-only**; silver never writes under it) |
| `SILVER_ROOT` | yes | — | Writable root for silver Parquet, **outside** `BRONZE_ROOT` (writes there are refused) |
| `SILVER_MONITOR_DIR` | no | — | Reserved for a future silver monitor; kept **outside** `SILVER_ROOT`. Unused today; unset → future silver checks no-op |

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
# grecohome-dagster — example environment for the silver, gold and Dagster-instance
# sections (each container reads only its own vars).
# Copy to .env for local dev. Production injects these via Ansible/secrets manager.

# --- Shared (grecohome-core BaseSubjectSettings) ---
# The bronze tree WearStream writes; read-only here.
BRONZE_ROOT=/data/bronze
LOG_LEVEL=INFO
ENVIRONMENT=development

# --- Silver (grecohome-silver); cross-subject layer, no source API/secrets ---
# Mount BRONZE_ROOT read-only here; SILVER_ROOT must be a separate path outside it.
SILVER_ROOT=/data/silver
# Reserved for the future silver monitor (unused today); MUST be outside SILVER_ROOT.
SILVER_MONITOR_DIR=/data/silver-monitor

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
