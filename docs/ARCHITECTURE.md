# Architecture

`grecohome-dagster` is a monorepo of personal data pipelines orchestrated by self-hosted
Dagster, in three layers. **Bronze** holds the raw source responses (the immutable source of
truth); it is written entirely by the external
[WearStream](#wearstream--the-bronze-producer) service, which captures Whoop and Garmin.
**Silver** reads bronze and writes typed, deduplicated Parquet. **Gold** reads silver and writes
analysis marts. Silver/gold are DuckDB-over-Parquet (no database), derived and fully
rebuildable; each is its own gRPC code location.

## Repository shape (uv workspace)

```
packages/
  core/    grecohome-core   — shared library (check helpers, silver helpers)
  silver/  grecohome-silver — silver layer (sleep, workouts, recovery, strain, daily, …)
  gold/    grecohome-gold   — gold layer (daily wellness mart, run monitors)
```

One root `pyproject.toml`, one `uv.lock`, one pinned Python (`.python-version`). Packages
depend on `grecohome-core` via the workspace. The load-bearing decisions are in the
[high-level design](high-level-design.md) and the design tree under [`intent/`](intent/).

## `grecohome-core` — the shared library

| Module | Responsibility |
|---|---|
| `config.py` | `BaseSubjectSettings` + `init_settings()` (friendly missing-var errors) |
| `logging_config.py` | structlog / JSON logging setup (tracebacks rendered without local variables) |
| `checks/` | `alerting_check`, the checks-only job/schedule builders, `table_freshness_check` (see [check helpers](intent/core/checks/checks-design.md)) |
| `silver/` | silver helpers: DuckDB connection, sidecar-safe bronze reading, dedup idiom, atomic Parquet write (`protected_root` guard) |

Nothing in core is table-specific; the silver helpers and check helpers are shared by every
silver and gold table.

## WearStream — the bronze producer

Whoop and Garmin capture lives in [WearStream](https://github.com/chainringlabs/wearstream), a
standalone, self-scheduling container outside this repo. It owns both sources end to end: Whoop
OAuth and the Garmin session, the trailing-window re-capture, skip-identical, backfill, its own
bronze checks and its own alerts. No code in this repo calls a source API.

- **Bronze read contract.** WearStream writes `whoop/` and `garmin/` under `BRONZE_ROOT`:
  `{source}/{collection}/dt=YYYY-MM-DD/` folders, payload files named with a 13-digit fetch time,
  a `.meta.json` sidecar beside each, `schema_version` `v1`. Silver's reader depends on exactly
  this (HLD: *Bronze is an external, read-only input*); a layout change is a WearStream release
  first, then a silver change.
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
  config.py          SilverSettings (silver_root + reserved silver_monitor_dir)
  sleep.py           Garmin + Whoop sleep column mapping (two co-equal sources)
  workouts.py        Garmin activities column mapping
  recovery.py        Whoop recovery column mapping (joins sleep)
  strain.py, daily.py, body.py, fitness.py, workout_splits.py, whoop_workouts.py
                     the other table families' column mappings
  dagster/           per-table assets + checks, daily jobs/schedules, definitions
```

One cross-subject code location (sleep spans two sources, so it can't live with either). Each
table reads `BRONZE_ROOT` via DuckDB and writes a typed, deduplicated Parquet under
`SILVER_ROOT` — a whole-table rebuild, idempotent, never written under bronze. Bronze upstreams
are declared by `AssetKey` (WearStream's external stubs); the reads are filesystem reads.
Tables:

- **`silver_sleep`** — FULL OUTER JOIN of `silver_sleep_garmin` + `silver_sleep_whoop` on the
  night; both devices side by side, neither authoritative. Whoop night = local wake date
  ([silver sleep design](intent/silver/sleep/sleep-design.md)).
- **`silver_workouts`** — one row per Garmin activity; laps in `silver_workout_splits`, Whoop's
  own log in `silver_whoop_workouts` ([silver workouts design](intent/silver/workouts/workouts-design.md)).
- **`silver_recovery`** — one row per Whoop cycle; joins sleep via `sleep_id`/`cycle_id` ([silver cycles design](intent/silver/cycles/cycles-design.md)).
  Plus the other derived tables (daily, strain, body, fitness, workout-splits, whoop-workouts).

Each table has `@asset_check`s (uniqueness/ranges = ERROR, coverage = WARN) and a daily rebuild
schedule; a shared checks-only job catches a stopped asset. Design: [docs/intent/silver/](intent/silver/silver-design.md).

## `grecohome-gold` — the gold layer

```
grecohome_gold/
  config.py          GoldSettings (silver_root + gold_root + reserved gold_monitor_dir)
  daily_wellness.py  the daily wellness mart SQL
  dagster/           assets + checks, daily job/schedule (after silver), run monitors, definitions
```

Analysis marts derived from silver — joins and per-day rollups silver deliberately deferred.
Reads `SILVER_ROOT`, writes `GOLD_ROOT` (never under silver). **`gold_daily_wellness`** — one
row per local day over a continuous spine, joining sleep, recovery, strain, daily activity,
Garmin and Whoop workouts and carried-forward weight, with `has_*` provenance. The gold code
location also hosts the instance-wide run monitors. Design: [docs/intent/gold/](intent/gold/gold-design.md).

## Orchestration model

- **Self-hosted Dagster.** The daemon + webserver run on the host. Each layer ships a gRPC
  **code-location image** (`dagster code-server start`) that the host registers via
  `workspace.yaml`. We never ship a Dagster instance/webserver. See [DEPLOYMENT](DEPLOYMENT.md).
- **Bronze `dt=` folders are UTC fetch-slices**, not local days, and hold every capture that
  differed from the one before. Silver applies local-day semantics at read time and keeps the
  latest capture per key — see the [HLD](high-level-design.md) (*Silver reads bronze as
  re-captured UTC fetch-slices*).
- **Silver and gold run on daily schedules** — silver rebuilds 06:00–06:55 UTC, after
  WearStream's daily Garmin capture; gold after it at 07:30 — reading whatever bronze holds at
  the time. Neither carries a concurrency pool: they make no source calls.
- **Backfill** happens in WearStream; silver and gold have no partitions, so after a bronze
  backfill their jobs are run on demand.

## Per-layer deployment

Silver and gold are each built into their own image (`ghcr.io/tgrecojr/grecohome-dagster-<name>`)
by a CI matrix, signed (cosign) with an SBOM + SLSA provenance. Layers deploy, fail, and scale
independently; silver mounts `BRONZE_ROOT` read-only and gold mounts `SILVER_ROOT` read-only
(see [DEPLOYMENT](DEPLOYMENT.md)). WearStream ships from its own repo and is deployed alongside,
by the host's Ansible.
