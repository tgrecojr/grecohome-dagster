# Deployment

Each code location — the Soil/USCRN bronze subject, silver and gold — ships as its own **gRPC
code-location image** that registers with the **existing host Dagster daemon + webserver**. We
don't *own* the Dagster instance, webserver, or `dagster.yaml` (the host does) — but because the
host uses the `DefaultRunLauncher`, each run executes **inside the code-location container**, so
that container does need the host's instance config + Postgres at run time (see
[Dagster instance wiring](#3-dagster-instance-wiring)).

Whoop and Garmin bronze is written by [WearStream](https://github.com/chainringlabs/wearstream),
a separate container deployed alongside these code locations — see
[Whoop and Garmin bronze (WearStream)](#whoop-and-garmin-bronze-wearstream).

## Images

Per-code-location images are published to GHCR on every push to `main`:

```
ghcr.io/tgrecojr/grecohome-dagster-soil:latest
ghcr.io/tgrecojr/grecohome-dagster-soil:soil-<sha>
ghcr.io/tgrecojr/grecohome-dagster-silver:latest     (silver-<sha>)
ghcr.io/tgrecojr/grecohome-dagster-gold:latest       (gold-<sha>)
```

Each image runs `dagster code-server start` serving its code location's `Definitions` on port
4000. Images are linux/amd64 only, cosign-signed, and ship an SBOM + SLSA
provenance attestation.

## Pins (hard requirement)

`dagster==1.13.30` and all `dagster-*==0.29.30` must match the host daemon/webserver so the
daemon ↔ code-location gRPC protocol stays in sync. A mismatch fails code-location loading
with opaque gRPC errors. Renovate opens one grouped `dagster` PR for these and never
automerges it; merge it by hand, in lockstep with the host image PR in `greco-home-builder`.

## Host wiring

### 1. Register the code locations (`workspace.yaml`)

On the host, point Dagster at each code location's gRPC server (Ansible-managed):

```yaml
load_from:
  - grpc_server: { host: dagster_soil,   port: 4000, location_name: soil_ingest }
  - grpc_server: { host: dagster_silver, port: 4000, location_name: silver }
  - grpc_server: { host: dagster_gold,   port: 4000, location_name: gold }
```

`host` is the container name on the shared docker network (`monitoring`).

### 2. Concurrency pool (`uscrn_api`)

The soil asset tags its op with the `uscrn_api` concurrency pool. **The limit is enforced by
the host instance (stored in its Postgres), not the image.** USCRN is low volume, so the limit
is optional; set it to 1 so the 6-hourly tick, the weekly correction run and any backfill fetch
NOAA's files one at a time. Silver and gold carry no pool.

In Dagster 1.13 a **per-pool limit is set in the instance DB via the CLI**, not in
`dagster.yaml`. (The `concurrency.pools` block in `dagster.yaml` only accepts
`default_limit` / `granularity` / `op_granularity_run_buffer` — naming a pool there, e.g.
`pools: { uscrn_api: { max_concurrent: 1 } }`, is invalid and crashes the daemon at instance
load.) Set it once, against the running instance (any container with `DAGSTER_HOME` + the
`DAGSTER_POSTGRES_*` env — the daemon works):

```bash
dagster instance concurrency set uscrn_api 1
dagster instance concurrency get uscrn_api   # verify
```

This is an **op**-granularity limit, so at most one `uscrn_api` op runs across all runs. It
persists in the instance DB (re-run it if you ever reset that DB). To keep it Ansible-managed,
run it as a post-deploy `docker_container_exec` against the daemon container:

```yaml
- name: Set uscrn_api pool concurrency limit
  community.docker.docker_container_exec:
    container: dagster_daemon
    command: dagster instance concurrency set uscrn_api 1
  changed_when: false
```

> A blanket `concurrency: { pools: { default_limit: 1 } }` in `dagster.yaml` is valid and
> would also cap `uscrn_api` while it's the only pool — but it limits *every* pool, so prefer
> the per-pool CLI for precision.

### 3. Dagster instance wiring

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

The pool limit from step 2 lives in the instance DB and is enforced by the daemon's run
coordinator, so the code-location containers need nothing extra for it.

### 4. Required environment / mounts

Inject per-code-location env at deploy (Ansible + secrets manager). See
`docs/ENV_TEMPLATE.md`. This section covers the bronze subject; silver and gold have their own
[host container](#host-container-dagster_silver) sections below.

App env (read by the app's settings):
- required: `BRONZE_ROOT`.
- optional: the `USCRN_*` tuning knobs (see [Soil](#soil--noaa-uscrn-bronze-subject)), and
  `BRONZE_MONITOR_DIR` — enables the bronze schema-drift check by giving it a writable place for
  baselines, **outside** `BRONZE_ROOT` (bronze stays immutable raw capture). Unset →
  schema-drift no-ops. See `docs/intent/core/checks/checks-design.md`.

Dagster instance env (read by Dagster, per step 3):
- required: `DAGSTER_HOME`, `DAGSTER_POSTGRES_USER`, `DAGSTER_POSTGRES_PASSWORD`,
  `DAGSTER_POSTGRES_HOST`, `DAGSTER_POSTGRES_DB`.

Mount these into the container:

- `BRONZE_ROOT` — writable; where raw captures are written.
- `DAGSTER_HOME` — the directory containing the shared `dagster.yaml`.
- `BRONZE_MONITOR_DIR` (optional) — writable; holds schema-drift baselines. Mount it as a
  **separate** volume from `BRONZE_ROOT` so checks never write under the immutable bronze tree
  (the check refuses to write a baseline that would land inside `BRONZE_ROOT`).

## Backfill

Backfill older bronze partitions through the same assets (no separate script):

```bash
dagster backfill --partition-range 2010-01-01...2010-12-31 --job uscrn_capture_job
```

The soil asset's backfill policy splits a range into runs of up to 366 partitions, so each run
fetches about one year file; the `uscrn_api` pool keeps a backfill from fetching alongside the
scheduled captures. Silver and gold have no partitions to backfill: after a bronze backfill, run
the affected silver job (then gold) on demand.

## Whoop and Garmin bronze (WearStream)

Whoop and Garmin capture lives in [WearStream](https://github.com/chainringlabs/wearstream), a
standalone, self-scheduling container — not a Dagster code location, so it has no
`workspace.yaml` entry, no Dagster schedules and no concurrency pool. It owns Whoop OAuth and
the Garmin session, the trailing-window re-capture, skip-identical, backfill, its own bronze
checks and its own alerts; re-authentication and Whoop/Garmin backfills are done through it.

- **Deployment** is in the host repo `greco-home-builder` (Ansible `tasks/wearstream.yml`). It
  writes `whoop/` and `garmin/` into the same host bronze tree that soil writes `uscrn/` into
  and silver reads.
- **Shared bronze contract.** Its files are byte-compatible with core's bronze writer (same
  layout, same `.meta.json` sidecar, `schema_version` `v1`). A layout change in
  `grecohome_core.bronze` needs a matching WearStream release before silver can rely on it.
- **Lineage stubs.** Silver declares `whoop_bronze_*` / `garmin_bronze_*` upstreams by
  `AssetKey`; no code location defines them, so Dagster shows them as external stubs. Silver
  runs on its own schedules regardless.
- **Alerts** are provisioned with WearStream's deployment in
  `files/grafana/provisioning/alerting/wearstream.yml` (greco-home-builder), not by this repo —
  see [Observability and alerting](#observability-and-alerting).

## Soil / NOAA USCRN (bronze subject)

Soil is **schedule-driven**, and the source is a **public NOAA file** (the `hourly02` USCRN
product) — **no auth, no secrets, no token/key mount**.

- **Image:** `ghcr.io/tgrecojr/grecohome-dagster-soil`, serving `grecohome_soil.dagster.definitions`.
- **Daily UTC partitions + row-slice + dedup.** The source is one ever-growing year file per station
  (`CRNH0203-{year}-{station}.txt`, one row/hour). Each daily partition's `uscrn_bronze_hourly` asset
  fetches the year file and stores **only that UTC date's rows** (`uscrn/hourly`, `dedupe=True`), so a
  few-times-a-day re-capture never re-stores the whole year. A finished day stores once; today
  re-writes only when a new row appears.
- **Schedules:** `uscrn_schedule` (every 6h, UTC) re-materializes the trailing `USCRN_LOOKBACK_DAYS`
  partitions; `uscrn_correction_schedule` (Sundays 03:30 UTC) re-slices the trailing
  `USCRN_CORRECTION_LOOKBACK_DAYS` (400) in one range run to pick up NOAA's late corrections. Older
  history is reachable via `dagster backfill` over the same asset. Design:
  `docs/intent/soil/soil-design.md`.

### Host container (`dagster_soil`)

On `monitoring`: `DAGSTER_HOME` + `DAGSTER_POSTGRES_*` (run worker → instance Postgres), the
`USCRN_*` env (`USCRN_STATION`, optional `USCRN_BASE_URL` / `USCRN_LOOKBACK_DAYS` /
`USCRN_CORRECTION_LOOKBACK_DAYS` / `USCRN_START_DATE`), and **two mounts** (no credential mount)
— the shared `dagster.yaml` (`DAGSTER_HOME`) and the bronze root (`/opt/datalake/bronze` →
`/data/bronze`; the app writes the `uscrn/` source segment itself) — plus the optional
`BRONZE_MONITOR_DIR` volume ([step 4](#4-required-environment--mounts)).

```yaml
# workspace.yaml
  - grpc_server: { host: dagster_soil, port: 4000, location_name: soil_ingest }
```

```bash
# optional concurrency pool (instance DB); low volume, not critical — see Host wiring step 2
dagster instance concurrency set uscrn_api 1
```

Enable `uscrn_schedule` and `uscrn_correction_schedule` in the UI (capture schedules start
stopped; the checks schedule `uscrn_bronze_checks_hourly` starts running). Their first ticks
capture the recent partitions; backfill the station's history with
`dagster backfill --partition-range 2010-01-01...<today> --job uscrn_capture_job`.

## Silver (cross-subject layer)

Silver is **not** a data subject — it captures nothing. It's a derived, rebuildable layer
that reads immutable bronze and writes typed, deduplicated **Parquet** for analysis. It
makes **no source-API calls** (no auth, no secrets, no token/key mount) and carries **no**
concurrency pool. Its tables (sleep, recovery, strain, workouts, weather, daily, body, fitness)
are designed in [docs/intent/silver/](intent/silver/silver-design.md).

- **Image:** `ghcr.io/tgrecojr/grecohome-dagster-silver`, serving
  `grecohome_silver.dagster.definitions`. Depends only on `grecohome-core` + DuckDB — no
  bronze source client is in the image.
- **Cross-code-location lineage:** each silver asset declares its bronze upstreams (for example
  `garmin_bronze_sleep`, `whoop_bronze_sleep`, `uscrn_bronze_hourly`) by `AssetKey`, so lineage
  renders across code locations. `uscrn_bronze_hourly` resolves to the soil location; the Whoop
  and Garmin keys are external stubs (WearStream writes those files). The reads themselves are
  **filesystem reads of `BRONZE_ROOT`**, not gRPC calls.
- **Whole-table rebuild.** Assets are unpartitioned; each run overwrites its Parquet from
  current bronze (idempotent — last run wins). Each family's daily rebuild runs between 06:00 and
  06:55 UTC over whatever bronze holds then; `silver_checks_daily` (07:05 UTC) runs every silver
  asset check independently. The rebuild schedules default to stopped and are enabled in the UI;
  the checks schedule starts running.

### Host container (`dagster_silver`)

Like soil — on `monitoring`, `DAGSTER_HOME` + `DAGSTER_POSTGRES_*` (run worker → instance
Postgres) — but the data mounts differ from a subject's: silver **reads** bronze and **writes** a
separate silver root.

- required app env: `BRONZE_ROOT`, `SILVER_ROOT`.
- optional app env: `USCRN_TIMEZONE` (the station's IANA tz for `silver_weather`'s local day;
  defaults to `America/New_York`), and `SILVER_MONITOR_DIR` — reserved for the forthcoming silver
  monitor/validation (mirrors `BRONZE_MONITOR_DIR`); unused today, declared + mounted now so
  turning it on needs no deploy change. Unset → future silver checks no-op.

Mounts (note bronze is **read-only** here — silver never writes under it):

- **`BRONZE_ROOT` — read-only** (`/opt/datalake/bronze` → `/data/bronze:ro`). Silver reads the
  source segments it projects (`garmin/`, `whoop/`, `uscrn/`).
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
source-API calls** (no auth/secrets) and carries **no** concurrency pool. Its marts
(`gold_daily_wellness`, `gold_daily_weather`) and the run monitors it hosts are designed in
[docs/intent/gold/](intent/gold/gold-design.md).

- **Image:** `ghcr.io/tgrecojr/grecohome-dagster-gold`, serving `grecohome_gold.dagster.definitions`.
  Depends only on `grecohome-core` + DuckDB.
- **Cross-code-location lineage:** the wellness mart declares its silver upstreams (`silver_sleep`,
  `silver_recovery`, `silver_workouts`, `silver_strain`, `silver_daily`, `silver_body`,
  `silver_whoop_workouts`) by `AssetKey`, and the weather mart declares `silver_weather`; the
  reads are **filesystem reads of `SILVER_ROOT`**, not gRPC calls.
- **Runs after silver.** `gold_wellness_daily` (07:30 UTC) and `gold_weather_daily` (07:40 UTC)
  rebuild the marts once silver's daily rebuilds (≤ 06:55) and silver checks (07:05) have run;
  `gold_checks_daily` (08:05 UTC) runs the gold checks (it starts running). Enable the rebuild
  schedules in the UI. On-demand: `dagster job execute --job gold_wellness_job`.

### Host container (`dagster_gold`)

Mirror silver — on `monitoring`, `DAGSTER_HOME` + `DAGSTER_POSTGRES_*` (run worker) — but the data
mounts are silver→gold:

- required app env: `SILVER_ROOT`, `GOLD_ROOT`.
- optional app env: `GOLD_WEATHER_MIN_VALID_HOURS` (the `gold_daily_weather` completeness gate,
  default 22), and `GOLD_MONITOR_DIR` — reserved for a future gold monitor (mirrors
  `*_MONITOR_DIR`); unused today, declared + mounted now so turning it on needs no deploy change.

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

Capture and rebuild schedules start **stopped**; switch these on in the Dagster UI once per code
location after its first deploy. Dagster remembers the state across redeploys. Checks schedules
and the run-monitor sensors start **running** and need nothing.

| Code location | Enable (capture/rebuild: starts stopped) | Already running (checks, monitors) |
|---|---|---|
| `soil_ingest` | `uscrn_schedule`, `uscrn_correction_schedule` | `uscrn_bronze_checks_hourly` |
| silver | `silver_sleep_daily`, `silver_workouts_daily`, `silver_recovery_daily`, `silver_weather_daily`, `silver_daily_daily`, `silver_strain_daily`, `silver_body_daily`, `silver_fitness_daily`, `silver_workout_splits_daily`, `silver_whoop_workouts_daily` | `silver_checks_daily` |
| gold | `gold_wellness_daily`, `gold_weather_daily` | `gold_checks_daily`; sensors `run_failure_monitor` and `run_queue_monitor` |

### Checks schedule minutes

The host is shared, and its own cron jobs run at the top of the hour, so every checks schedule
takes its own off-minute and no two tick at the same time (`docs/intent/core/checks/checks-design.md`,
*Checks-only job and schedule*). The core builder refuses a cron whose minute field is `0`, `*`,
a step or a list. When adding a code location, pick a minute not in this table.

| Schedule | Cron (UTC) | Ticks |
|---|---|---|
| `uscrn_bronze_checks_hourly` | `43 * * * *` | hourly at :43 |
| `silver_checks_daily` | `5 7 * * *` | 07:05 |
| `gold_checks_daily` | `5 8 * * *` | 08:05 |

For reference, the capture and rebuild schedules: Soil at :00 every 6 h (plus Sunday 03:30),
silver rebuilds 06:00–06:55, gold rebuilds 07:30 and 07:40.

## Observability and alerting

Design: `docs/high-level-design.md` (*Failures page through Loki and Grafana to Slack*) and
`docs/intent/core/checks/checks-design.md` (*Alerting*).

- **Logs:** every code location's structlog output ships to Loki, labelled
  `service_name="dagster_<location>"` (for example `dagster_soil`, `dagster_silver`).
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
- **Checks-only schedules start running** on deploy: `uscrn_bronze_checks_hourly`,
  `silver_checks_daily` and `gold_checks_daily`. One stopped deliberately in the UI stays
  stopped.
- **Run monitoring (host `dagster.yaml`):** `run_monitoring` is enabled with
  `max_runtime_seconds: 1800`, so a run executing for more than 30 minutes is terminated as
  wedged. `start_timeout_seconds` and `cancel_timeout_seconds` are 300, and runs aren't
  auto-resumed. This is the platform backstop for a stuck run holding a concurrency pool.

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
docker build -f packages/soil/Dockerfile -t grecohome-dagster-soil:dev .
docker run --rm \
  -e BRONZE_ROOT=/data/bronze \
  -v /local/bronze:/data/bronze \
  -p 4000:4000 grecohome-dagster-soil:dev
# health: docker exec <id> dagster api grpc-health-check -p 4000
```

Swap `soil` for `silver` or `gold` (and their roots: `BRONZE_ROOT` + `SILVER_ROOT`, or
`SILVER_ROOT` + `GOLD_ROOT`) to build the other layers.
