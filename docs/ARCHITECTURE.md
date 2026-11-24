# Architecture

`grecohome-dagster` is a monorepo of personal data pipelines orchestrated by self-hosted
Dagster, in three layers. **Bronze** holds the raw source responses (the immutable source of
truth): the Soil/USCRN subject captures NOAA's station files here, and the external
[WearStream](#wearstream--the-external-whoop-and-garmin-producer) service writes Whoop and
Garmin into the same tree. **Silver** reads bronze and writes typed, deduplicated Parquet.
**Gold** reads silver and writes analysis marts. Silver/gold are DuckDB-over-Parquet (no
database), derived and fully rebuildable; each layer is its own gRPC code location.

## Repository shape (uv workspace)

```
packages/
  core/    grecohome-core   — shared framework (bronze capture + checks, silver helpers, dagster)
  soil/    grecohome-soil   — NOAA USCRN soil/temp bronze subject (ported from soildata)
  silver/  grecohome-silver — silver layer (sleep, workouts, recovery, strain, weather, …)
  gold/    grecohome-gold   — gold layer (daily wellness and daily weather marts)
```

One root `pyproject.toml`, one `uv.lock`, one pinned Python (`.python-version`). Packages
depend on `grecohome-core` via the workspace. The load-bearing decisions are in the
[high-level design](high-level-design.md) and the design tree under [`intent/`](intent/).

## `grecohome-core` — the shared framework

| Module | Responsibility |
|---|---|
| `bronze/capture.py` | Atomic, append-only raw-payload capture; opt-in content-hash dedup (`dedupe`) + explicit `ext` |
| `config.py` | `BaseSubjectSettings` + `init_settings()` (friendly missing-var errors) |
| `logging_config.py` | structlog / JSON logging setup (tracebacks rendered without local variables) |
| `dagster/helpers.py` | `daily_utc_partitions`, `trailing_partition_keys` |
| `checks/` | bronze asset-check families + builders (see [bronze checks](intent/core/checks/checks-design.md)) |
| `silver/` | silver helpers: DuckDB connection, sidecar-safe bronze reading (JSON + CSV), dedup idiom, atomic Parquet write (`protected_root` guard) |

Nothing in core is source-specific; a new subject reuses the bronze framework, and the silver
helpers are shared by every silver/gold table. The layout `bronze/capture.py` writes is also the
contract WearStream writes to ([below](#wearstream--the-external-whoop-and-garmin-producer)).

## `grecohome-soil` — the bronze subject

```
grecohome_soil/
  config.py          SoilSettings(BaseSubjectSettings)
  fetch.py           build the year-file URL, GET it (httpx), slice a UTC date's rows
  capture.py         adapter over core capture_bronze (uscrn/hourly, dedupe=True)
  dagster/
    assets.py        uscrn_bronze_hourly (daily UTC partitions, end_offset=1)
    checks.py        bronze asset checks + the checks-only job/schedule
    schedules.py     6-hourly trailing-window + weekly correction schedules, the capture job
    definitions.py   the gRPC code-location target
```

A public NOAA year file (no auth), stored as one UTC day's rows per partition, re-captured every
6 h plus a weekly 400-day correction sweep for NOAA's late in-place corrections. Authoritative
design (capture, schedules, checks, and why): `docs/intent/soil/soil-design.md`.

### Data flow (per run)

```
Dagster schedule (uscrn_schedule, every 6 h, UTC)
  └─ RunRequest(partition_key=YYYY-MM-DD) for each trailing partition
       └─ uscrn_bronze_hourly (pool: uscrn_api)
            └─ fetch the station's year file(s) once per run → slice the UTC date's rows
                 └─ capture_hourly → capture_bronze(rows + sidecar, dedupe=True) → BRONZE_ROOT
```

The weekly `uscrn_correction_schedule` drives the same asset over a contiguous partition range
in one run. The asset does no persistence beyond bronze; row and capture counts are attached as
run metadata.

## WearStream — the external Whoop and Garmin producer

Whoop and Garmin capture lives in [WearStream](https://github.com/chainringlabs/wearstream), a
standalone, self-scheduling container outside this repo. It owns both sources end to end: Whoop
OAuth and the Garmin session, the trailing-window re-capture, skip-identical, backfill, its own
bronze checks and its own alerts. No code in this repo calls the Whoop or Garmin APIs.

- **Shared bronze contract.** WearStream writes `whoop/` and `garmin/` under the same
  `BRONZE_ROOT`, byte-compatible with core's `bronze/capture.py`: same folder layout, same
  `.meta.json` sidecar, `schema_version` `v1`. A layout change in the core writer needs a
  matching WearStream release before silver can rely on it, and silver's readers keep reading
  both producers' files ([bronze writer](intent/core/bronze/bronze-design.md)).
- **AssetKey stubs.** Silver declares lineage on the `whoop_bronze_*` and `garmin_bronze_*`
  `AssetKey`s. No code location defines them, so Dagster renders them as external stubs. Silver
  runs on its own schedules and never waits on a bronze materialization event.
- **Deployment and alerts** belong to the host repo `greco-home-builder`: the Ansible task
  `tasks/wearstream.yml`, and the Grafana rules in
  `files/grafana/provisioning/alerting/wearstream.yml` ("WearStream needs attention" on any
  `ERROR` log line, "WearStream silent" when it logs nothing for 2 h).

## `grecohome-silver` — the silver layer

```
grecohome_silver/
  config.py          SilverSettings (silver_root + reserved silver_monitor_dir + uscrn_timezone)
  sleep.py           Garmin + Whoop sleep column mapping (two co-equal sources)
  workouts.py        Garmin activities column mapping
  recovery.py        Whoop recovery column mapping (joins sleep)
  strain.py, daily.py, body.py, fitness.py, weather.py, workout_splits.py, whoop_workouts.py
                     the other table families' column mappings
  dagster/           per-table assets + checks, daily jobs/schedules, definitions
```

One cross-subject code location (sleep spans two sources, so it can't live with either). Each
table reads `BRONZE_ROOT` via DuckDB and writes a typed, deduplicated Parquet under
`SILVER_ROOT` — a whole-table rebuild, idempotent, never written under bronze. Bronze upstreams
are declared by `AssetKey` (cross-location lineage; the Whoop and Garmin keys are WearStream's
external stubs); the reads are filesystem reads. Tables:

- **`silver_sleep`** — FULL OUTER JOIN of `silver_sleep_garmin` + `silver_sleep_whoop` on the
  night; both devices side by side, neither authoritative. Whoop night = local wake date
  ([silver sleep design](intent/silver/sleep/sleep-design.md)).
- **`silver_workouts`** — one row per Garmin activity; laps in `silver_workout_splits`, Whoop's
  own log in `silver_whoop_workouts` ([silver workouts design](intent/silver/workouts/workouts-design.md)).
- **`silver_recovery`** — one row per Whoop cycle; joins sleep via `sleep_id`/`cycle_id` ([silver cycles design](intent/silver/cycles/cycles-design.md)).
  Plus the other derived tables (weather, daily, strain, body, fitness, workout-splits,
  whoop-workouts).

Each table has `@asset_check`s (uniqueness/ranges = ERROR, coverage = WARN) and a daily rebuild
schedule; a shared checks-only job catches a stopped asset. Design: [docs/intent/silver/](intent/silver/silver-design.md).

## `grecohome-gold` — the gold layer

```
grecohome_gold/
  config.py          GoldSettings (silver_root + gold_root + reserved gold_monitor_dir)
  daily_wellness.py  the daily wellness mart SQL
  daily_weather.py   the daily weather mart SQL
  dagster/           assets + checks, daily jobs/schedules (after silver), run monitors, definitions
```

Analysis marts derived from silver — joins and per-day rollups silver deliberately deferred.
Reads `SILVER_ROOT`, writes `GOLD_ROOT` (never under silver). **`gold_daily_wellness`** — one
row per local day over a continuous spine, joining sleep, recovery, strain, daily activity,
Garmin and Whoop workouts and carried-forward weight, with `has_*` provenance;
**`gold_daily_weather`** — the gardening rollup of `silver_weather`. The gold code location also
hosts the instance-wide run monitors. Design: [docs/intent/gold/](intent/gold/gold-design.md).

## Orchestration model

- **Self-hosted Dagster.** The daemon + webserver run on the host. Each layer ships a gRPC
  **code-location image** (`dagster code-server start`) that the host registers via
  `workspace.yaml`. We never ship a Dagster instance/webserver. See [DEPLOYMENT](DEPLOYMENT.md).
- **Daily UTC partitions** in bronze (`end_offset=1` so the in-progress day is materializable).
  A partition is a UTC *fetch-slice*, not a semantic local day — see the
  [HLD](high-level-design.md) (*Bronze partitions are daily UTC fetch-slices*).
- **Trailing-window re-capture.** `uscrn_schedule` (every 6 h) re-materializes the trailing
  `USCRN_LOOKBACK_DAYS` partitions, and `uscrn_correction_schedule` (Sundays) re-slices the
  trailing `USCRN_CORRECTION_LOOKBACK_DAYS` in one range run, so NOAA's late corrections are
  eventually re-captured. Correctness comes from the trailing window + content-hash dedup, not
  the cadence.
- **One `uscrn_api` concurrency pool** tags the soil asset (limit set on the host instance;
  optional at this volume). Silver and gold make no source calls and carry no pool.
- **Silver and gold run on daily schedules** — silver rebuilds 06:00–06:55 UTC, gold after it at
  07:30 and 07:40 — reading whatever bronze holds at the time.
- **Backfill** = `dagster backfill` over the same bronze assets; silver and gold have no
  partitions, so after a backfill their jobs are run on demand.

## Per-layer deployment

Each layer — the bronze subject, plus silver and gold — is built into its own image
(`ghcr.io/tgrecojr/grecohome-dagster-<name>`) by a CI matrix, signed (cosign) with an SBOM +
SLSA provenance. Layers deploy, fail, and scale independently; silver mounts `BRONZE_ROOT`
read-only and gold mounts `SILVER_ROOT` read-only (see [DEPLOYMENT](DEPLOYMENT.md)). WearStream
ships from its own repo and is deployed alongside, by the host's Ansible.
