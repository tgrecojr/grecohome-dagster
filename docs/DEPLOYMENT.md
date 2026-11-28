# Deployment

Each code location — silver and gold — ships as its own **gRPC
code-location image** that registers with the **existing host Dagster daemon + webserver**. We
don't *own* the Dagster instance, webserver, or `dagster.yaml` (the host does) — but because the
host uses the `DefaultRunLauncher`, each run executes **inside the code-location container**, so
that container does need the host's instance config + Postgres at run time (see
[Dagster instance wiring](#2-dagster-instance-wiring)).

All bronze is written by [WearStream](https://github.com/chainringlabs/wearstream), a separate
container deployed alongside these code locations — see
[Whoop and Garmin bronze (WearStream)](#whoop-and-garmin-bronze-wearstream).

## Images

Per-code-location images are published to GHCR on every push to `main`:

```
ghcr.io/tgrecojr/grecohome-dagster-silver:latest     (silver-<sha>)
ghcr.io/tgrecojr/grecohome-dagster-gold:latest       (gold-<sha>)
```

Each image runs `dagster code-server start` serving its code location's `Definitions` on port
4000. Images are linux/amd64 only, cosign-signed, and ship an SBOM + SLSA
provenance attestation.

## Pins (hard requirement)

`dagster==1.13.31` and all `dagster-*==0.29.31` must match the host daemon/webserver so the
daemon ↔ code-location gRPC protocol stays in sync. A mismatch fails code-location loading
with opaque gRPC errors. Renovate opens one grouped `dagster` PR for these and never
automerges it; merge it by hand, in lockstep with the host image PR in `greco-home-builder`.

## Host wiring

### 1. Register the code locations (`workspace.yaml`)

On the host, point Dagster at each code location's gRPC server (Ansible-managed):

```yaml
load_from:
  - grpc_server: { host: dagster_silver, port: 4000, location_name: silver }
  - grpc_server: { host: dagster_gold,   port: 4000, location_name: gold }
```

`host` is the container name on the shared docker network (`monitoring`).

### 2. Dagster instance wiring

Because the host uses the `DefaultRunLauncher` (no `run_launcher` block), each run executes
as a subprocess **inside the code-location container**, and that subprocess writes to the
host instance's Postgres event/run/schedule storage. So the container must share the host's
instance config:

- **Mount the daemon's `dagster.yaml`** into the container and set **`DAGSTER_HOME`** to its
  directory. It's the same file the daemon uses (carries no secrets — it references DB creds
  by env-var name). Don't bake it into the image.
- **Provide the Postgres env vars** the `dagster.yaml` references:
  `DAGSTER_POSTGRES_USER`, `DAGSTER_POSTGRES_PASSWORD`, `DAGSTER_POSTGRES_HOST`,
  `DAGSTER_POSTGRES_DB` (port is set in `dagster.yaml`). The container must be able to reach
  that Postgres.
- `dagster-postgres` is bundled in the image, so the run subprocess can instantiate the
  Postgres storage.
- **Pin the DB driver in `dagster.yaml`** with `scheme: postgresql+psycopg2` under
  `storage.postgres.postgres_db`. `dagster-postgres` ships only psycopg2. SQLAlchemy 2.1
  changed the default driver for a bare `postgresql://` URL to psycopg (v3). With the scheme
  left at its default, every code location fails at startup with `No module named 'psycopg'`.
  That was the 2026-09-29 outage. `packages/core/tests/test_instance_storage.py` mirrors the
  host's storage config and checks in CI that its driver is importable, so keep the two in
  sync.

### 3. Environment and mounts

Inject per-code-location env at deploy (Ansible + secrets manager); see `docs/ENV_TEMPLATE.md`.
Each layer's app env and mounts are in its [silver](#host-container-dagster_silver) and
[gold](#host-container-dagster_gold) host-container section. Every container also needs the
Dagster instance env from step 2: `DAGSTER_HOME`, `DAGSTER_POSTGRES_USER`,
`DAGSTER_POSTGRES_PASSWORD`, `DAGSTER_POSTGRES_HOST`, `DAGSTER_POSTGRES_DB`.

## Backfill

Bronze backfills happen in WearStream. Silver and gold have no partitions to backfill: after a
bronze backfill, run the affected silver job (then gold) on demand.

## Whoop and Garmin bronze (WearStream)

Whoop and Garmin capture lives in [WearStream](https://github.com/chainringlabs/wearstream), a
standalone, self-scheduling container — not a Dagster code location, so it has no
`workspace.yaml` entry, no Dagster schedules and no concurrency pool. It owns Whoop OAuth and
the Garmin session, the trailing-window re-capture, skip-identical, backfill, its own bronze
checks and its own alerts; re-authentication and Whoop/Garmin backfills are done through it.

- **Deployment** is in the host repo `greco-home-builder` (Ansible `tasks/wearstream.yml`). It
  writes `whoop/` and `garmin/` into the host bronze tree that silver reads.
- **Bronze read contract.** Silver's reader depends on WearStream's layout (`dt=` folders, the
  `.meta.json` sidecar, `schema_version` `v1`; HLD: *Bronze is an external, read-only input*). A
  layout change is a WearStream release first, then a silver change.
- **Lineage stubs.** Silver declares `whoop_bronze_*` / `garmin_bronze_*` upstreams by
  `AssetKey`; no code location defines them, so Dagster shows them as external stubs. Silver
  runs on its own schedules regardless.
- **Alerts** are provisioned with WearStream's deployment in
  `files/grafana/provisioning/alerting/wearstream.yml` (greco-home-builder), not by this repo —
  see [Observability and alerting](#observability-and-alerting).

## Silver (cross-subject layer)

Silver is **not** a data subject — it captures nothing. It's a derived, rebuildable layer
that reads immutable bronze and writes typed, deduplicated **Parquet** for analysis. It
makes **no source-API calls** (no auth, no secrets, no token/key mount) and carries **no**
concurrency pool. Its tables (sleep, recovery, strain, workouts, daily, body, fitness)
are designed in [docs/intent/silver/](intent/silver/silver-design.md).

- **Image:** `ghcr.io/tgrecojr/grecohome-dagster-silver`, serving
  `grecohome_silver.dagster.definitions`. Depends only on `grecohome-core` + DuckDB — no
  bronze source client is in the image.
- **Cross-code-location lineage:** each silver asset declares its bronze upstreams (for example
  `garmin_bronze_sleep`, `whoop_bronze_sleep`) by `AssetKey`, so lineage renders in the UI. No
  code location defines those keys, so they are external stubs (WearStream writes those files). The reads themselves are
  **filesystem reads of `BRONZE_ROOT`**, not gRPC calls.
- **Whole-table rebuild.** Assets are unpartitioned; each run overwrites its Parquet from
  current bronze (idempotent — last run wins). Each family's daily rebuild runs between 06:00 and
  06:55 UTC over whatever bronze holds then; `silver_checks_daily` (07:05 UTC) runs every silver
  asset check independently. The rebuild schedules default to stopped and are enabled in the UI;
  the checks schedule starts running.

### Host container (`dagster_silver`)

On `monitoring`, with `DAGSTER_HOME` + `DAGSTER_POSTGRES_*` (run worker → instance Postgres).
Silver **reads** bronze and **writes** a separate silver root.

- required app env: `BRONZE_ROOT`, `SILVER_ROOT`.
- optional app env: `SILVER_MONITOR_DIR` — reserved for a future silver monitor; unused today,
  declared + mounted now so turning it on needs no deploy change. Unset → future silver checks
  no-op.

Mounts (note bronze is **read-only** here — silver never writes under it):

- **`BRONZE_ROOT` — read-only** (`/opt/datalake/bronze` → `/data/bronze:ro`). Silver reads the
  source segments it projects (`garmin/`, `whoop/`).
- **`SILVER_ROOT` — writable**, a **separate** volume outside bronze (e.g.
  `/opt/datalake/silver` → `/data/silver`). The atomic Parquet writer refuses any path under
  `BRONZE_ROOT`.
- **`SILVER_MONITOR_DIR`** (optional) — writable, **separate** from `SILVER_ROOT` (e.g.
  `/opt/docker/dagster/silver/monitor` → `/monitor/silver`), so future checks never write
  under the silver Parquet tree.
- `DAGSTER_HOME` — the directory containing the shared `dagster.yaml`.

```yaml
# workspace.yaml
  - grpc_server: { host: dagster_silver, port: 4000, location_name: silver }
```

No concurrency pool is needed — silver makes no source calls. To rebuild on demand (e.g.
after a bronze backfill), materialize the job rather than backfilling partitions:

```bash
dagster job execute --job silver_sleep_job   # whole-table rebuild from current bronze
```

## Gold (cross-layer marts)

Gold is the analysis layer — derived, rebuildable marts built **from silver** (not bronze).
It reads `SILVER_ROOT` and writes marts under a new `GOLD_ROOT`; like silver it makes **no
source-API calls** (no auth/secrets) and carries **no** concurrency pool. Its mart
(`gold_daily_wellness`) and the run monitors it hosts are designed in
[docs/intent/gold/](intent/gold/gold-design.md).

- **Image:** `ghcr.io/tgrecojr/grecohome-dagster-gold`, serving `grecohome_gold.dagster.definitions`.
  Depends only on `grecohome-core` + DuckDB.
- **Cross-code-location lineage:** the wellness mart declares its silver upstreams (`silver_sleep`,
  `silver_recovery`, `silver_workouts`, `silver_strain`, `silver_daily`, `silver_body`,
  `silver_whoop_workouts`) by `AssetKey`; the reads are **filesystem reads of `SILVER_ROOT`**, not gRPC calls.
- **Runs after silver.** `gold_wellness_daily` (07:30 UTC) rebuilds the mart once silver's daily rebuilds (≤ 06:55) and silver checks (07:05) have run;
  `gold_checks_daily` (08:05 UTC) runs the gold checks (it starts running). Enable the rebuild
  schedule in the UI. On-demand: `dagster job execute --job gold_wellness_job`.

### Host container (`dagster_gold`)

Mirror silver — on `monitoring`, `DAGSTER_HOME` + `DAGSTER_POSTGRES_*` (run worker) — but the data
mounts are silver→gold:

- required app env: `SILVER_ROOT`, `GOLD_ROOT`.
- optional app env: `GOLD_MONITOR_DIR` — reserved for a future gold monitor (mirrors
  `SILVER_MONITOR_DIR`); unused today, declared + mounted now so turning it on needs no deploy
  change.

Mounts (silver is **read-only** here — gold never writes under it):

- **`SILVER_ROOT` — read-only** (`/opt/datalake/silver` → `/data/silver:ro`).
- **`GOLD_ROOT` — writable**, a **separate** volume outside silver (e.g. `/opt/datalake/gold` →
  `/data/gold`). The atomic writer refuses any path under `SILVER_ROOT`.
- **`GOLD_MONITOR_DIR`** (optional) — writable, separate from `GOLD_ROOT`.
- `DAGSTER_HOME` — the directory containing the shared `dagster.yaml`.

```yaml
# workspace.yaml
  - grpc_server: { host: dagster_gold, port: 4000, location_name: gold }
```

No concurrency pool is needed — gold makes no source calls.

## Schedules and sensors to enable after a first deploy

Rebuild schedules start **stopped**; switch these on in the Dagster UI once per code
location after its first deploy. Dagster remembers the state across redeploys. Checks schedules
and the run-monitor sensors start **running** and need nothing.

| Code location | Enable (rebuild: starts stopped) | Already running (checks, monitors) |
|---|---|---|
| silver | `silver_sleep_daily`, `silver_workouts_daily`, `silver_recovery_daily`, `silver_daily_daily`, `silver_strain_daily`, `silver_body_daily`, `silver_fitness_daily`, `silver_workout_splits_daily`, `silver_whoop_workouts_daily` | `silver_checks_daily` |
| gold | `gold_wellness_daily` | `gold_checks_daily`; sensors `run_failure_monitor` and `run_queue_monitor` |

### Checks schedule minutes

The host is shared, and its own cron jobs run at the top of the hour, so every checks schedule
takes its own off-minute and no two tick at the same time (`docs/intent/core/checks/checks-design.md`,
*Checks-only job and schedule*). The core builder refuses a cron whose minute field is `0`, `*`,
a step or a list. When adding a code location, pick a minute not in this table.

| Schedule | Cron (UTC) | Ticks |
|---|---|---|
| `silver_checks_daily` | `5 7 * * *` | 07:05 |
| `gold_checks_daily` | `5 8 * * *` | 08:05 |

For reference, the rebuild schedules: silver rebuilds 06:00–06:55, gold rebuilds 07:30.

## Observability and alerting

Design: `docs/high-level-design.md` (*Failures page through Loki and Grafana to Slack*) and
`docs/intent/core/checks/checks-design.md` (*Alerting*).

- **Logs:** every code location's structlog output ships to Loki, labelled
  `service_name="dagster_<location>"` (for example `dagster_silver`, `dagster_gold`).
- **Grafana alert rules → Slack contact point.** The Slack credential lives only in Grafana.

  | Rule | Fires on | For |
  |---|---|---|
  | Dagster asset check failing | any `asset_check_failed` line (a failing ERROR check, any layer) | 10 m |
  | Dagster run failing | any `dagster_run_failed` line (gold's `run_failure_monitor`, any code location), grouped by job | immediately |
  | Dagster run queue backed up | `run_queue_backed_up` (gold's queue-depth monitor) | 30 m |

  WearStream pages through the same Loki → Grafana → Slack path with its own rules ("WearStream
  needs attention" on any `ERROR` line, "WearStream silent" after 2 h with no log lines), which
  are provisioned with its deployment in greco-home-builder rather than by this repo.
- **Dashboard:** "Data Platform Health" (check failures, queue backups, per-code-location log
  volume).
- **Checks-only schedules start running** on deploy: `silver_checks_daily` and
  `gold_checks_daily`. One stopped deliberately in the UI stays
  stopped.
- **Run monitoring (host `dagster.yaml`):** `run_monitoring` is enabled with
  `max_runtime_seconds: 1800`, so a run executing for more than 30 minutes is terminated as
  wedged. `start_timeout_seconds` and `cancel_timeout_seconds` are 300, and runs aren't
  auto-resumed. This is the platform backstop for a stuck run.

## Grafana (data-lake dashboards)

Grafana reads the lake **directly** — no query service, no Postgres. The Grafana
container mounts the lake roots read-only (`/data/bronze`, `/data/silver`, `/data/gold`)
and a **DuckDB datasource plugin** (`motherduck-duckdb-datasource`, in-memory/local mode)
runs `read_parquet('/data/<layer>/...')` straight off those mounts. Each panel is a SQL
query, so the daily silver/gold rebuilds show up with no restart.

- **Mounts (read-only):** `/data/bronze`, `/data/silver`, `/data/gold` into the Grafana
  container. The read-only, lake-only mount is the security boundary (a query can only
  ever read those paths).
- **Panel target shape:** `format` must be the **integer** enum (`1` = table), not the
  string `"table"` — the plugin's Go backend rejects a string with
  *"cannot unmarshal string into Go struct field Query.format"*.
- **Dashboards:** `Daily Wellness` (`/d/daily-wellness`, on `gold/wellness`) and `Sleep`
  (`/d/sleep-lake`, on `silver/sleep`), both in the *Health (data lake)* folder.

> A standalone `grecohome-lakequery` HTTP service (DuckDB-over-Parquet on :9999) was the
> earlier serving approach; it was retired in favor of the direct DuckDB datasource above.
> Resurrect it from git history if a remote/HTTP query boundary is ever needed.

## Building locally

```bash
docker build -f packages/silver/Dockerfile -t grecohome-dagster-silver:dev .
docker run --rm \
  -e BRONZE_ROOT=/data/bronze -e SILVER_ROOT=/data/silver \
  -v /local/bronze:/data/bronze:ro -v /local/silver:/data/silver \
  -p 4000:4000 grecohome-dagster-silver:dev
# health: docker exec <id> dagster api grpc-health-check -p 4000
```

Swap `silver` for `gold` (and its roots: `SILVER_ROOT` + `GOLD_ROOT`) to build the gold layer.
