---
parent: high-level-design
prefix: SILVER
---

# Silver layer

## Context

Silver is the typed, deduplicated, analysis-ready projection of bronze. For each table it reads
the raw bronze payloads, extracts the **true event date** from the payload, unnests and types
fields, deduplicates to **one row per logical record**, and writes Parquet. Silver is derived
and rebuildable: it can be dropped and regenerated from bronze at any time, so it is neither
immutable nor precious. **Bronze stays the only source of truth**, and silver never writes to it.

Silver is one cross-subject code location (`grecohome_silver`, its own image), because some
tables join subjects (sleep joins Garmin and Whoop). Gold reads silver's Parquet.

Silver does no analysis: no aggregates across days, correlations or scores of its own. Those
belong to gold.

## Layer rules

- **Rebuilt every run.** Each materialization fully overwrites its table from current bronze:
  last run wins, with no append, merge or history of its own.
- **Outside bronze.** Silver writes only under `SILVER_ROOT`. The shared writer refuses any
  destination inside `BRONZE_ROOT`, and bronze is mounted read-only in the silver container.
- **Swappable root.** `SILVER_ROOT` comes from config, mirroring the bronze convention, so an
  object-store migration stays open.
- **Sidecars are never read as payloads.** The shared reader drops `*.meta.json` before DuckDB
  sees a file.
- **The event date comes from the payload, never the bronze `dt=` folder.** Bronze partitions
  are UTC fetch slices or capture dates; each table derives its own event date and local-day
  meaning (HLD: *Bronze partitions are daily UTC fetch-slices*).
- **Bronze is re-captured, so deduplication is mandatory.** Every table that reads a
  re-captured collection keeps the latest capture or restatement per logical key, and tolerates
  the same record appearing in several captures or folders.
- **No source calls, no `*_api` pool.** Silver reads files only, so its assets and checks never
  contend with ingestion.
- **A bad read fails the rebuild loudly.** A bronze payload that can't be parsed, or a payload
  shape a table can't convert, fails that table's run. The previous Parquet stays in place,
  because the write is atomic, so a failed rebuild leaves the last good table rather than a
  partial one.

## Architecture

- **Generic machinery lives in core** (`grecohome_core.silver`, see
  [core silver helpers](../core/silver/silver-design.md)): an in-memory DuckDB connection,
  sidecar-safe readers for JSON, CSV and line-oriented bronze, the
  `row_number() … = 1` dedup idiom, JSON-path helpers, and the atomic Parquet writer guarded by
  `protected_root`.
- **Each table family's column mapping lives in one silver module**, the only place payload
  field names appear (for example `grecohome_silver.sleep`).
- **Bronze upstreams are declared by `AssetKey`** for cross-code-location lineage. The reads are
  filesystem reads of `BRONZE_ROOT` through DuckDB, never calls into the subject code locations,
  so the silver image includes no subject package. DuckDB lives only in the silver and gold
  images; the bronze capture images carry no query engine.
- **Payloads are read as raw JSON text** and extracted by JSON path, so a missing or renamed key
  yields `NULL` rather than an error: heterogeneous, drifting bronze never crashes a rebuild.

## Storage

- **Parquet (zstd)**, written with DuckDB `COPY … TO … (FORMAT parquet, COMPRESSION zstd)` to a
  temporary file in the destination directory, then renamed into place, so a crashed run never
  leaves a half-written table.
- **Layout:** `{SILVER_ROOT}/<family>/<table>.parquet`. A table's source intermediates use a
  leading underscore (for example `sleep/_garmin.parquet`).
- **No partitioning.** Each asset is a whole-table rebuild reading every bronze partition. The
  data is small (thousands of nights, about 55k glucose readings). Partition by year only if it
  grows. After a bronze backfill, the affected family's job is run on demand; there are no silver
  partitions to backfill (`docs/DEPLOYMENT.md`).

## Asset checks

Every table has `@asset_check`s, following the bronze convention:

- **Structural correctness is ERROR:** a unique, non-null key; values in plausible ranges (which
  catches unit and parsing bugs); and the table's Parquet existing.
- **Table freshness is ERROR.** Each table has a freshness check that fails when its Parquet
  file was last written more than **36 hours** ago (the file's modification time). A failed
  rebuild leaves the previous table in place, where every other check still passes, and a failed
  run pages no one, so freshness is what makes a failed or never-enabled rebuild loud. At the
  daily 07:00 check a table is about 1 hour old normally, about 25 hours old after one missed
  rebuild and about 49 after two, so 36 hours pages on the **second consecutive** miss, with
  wide margin on both sides. One missed day costs little (silver is rebuildable and the next
  rebuild catches up), and a missed rebuild that persists still pages within about two days.
- **Coverage and expectation drift is WARN:** silver row counts against bronze's distinct keys,
  and source mix. These stay in the UI and don't page.
- **Every silver check is wrapped in `alerting_check`,** so any check result that fails at ERROR
  pages. That includes a WARN check whose table's Parquet is missing: a missing table is an ERROR
  failure for every check on it.
- **No concurrency pool.** A checks-only job (`silver_checks_job`) runs every table's checks
  daily at **07:00 UTC** (`silver_checks_daily`, which starts running), so a stopped rebuild is
  still caught. Checks also run after each table's rebuild.

## Schedules

Each table family has its own job and **daily** rebuild schedule, staggered between **06:00 and
06:55 UTC** (sleep at 06:00). Rebuild schedules start **stopped**, like all capture and build
schedules (HLD), and are listed in `docs/DEPLOYMENT.md`. A daily cadence is enough: silver is a
whole-table projection, with nothing intraday to chase.

The window sits **after the day's bronze captures**: Garmin's daily tick (05:30 UTC), Whoop's
hourly ticks (:17) and USCRN's 6-hourly ticks. A bronze capture that lands after silver's
rebuild reaches silver, gold and the dashboards a day late, so a capture schedule that moves
past 06:00 UTC means moving this window too. Gold's rebuilds follow silver's checks.

## Table families

| Family | Doc | Tables | Status |
|---|---|---|---|
| Sleep | [sleep/sleep-design.md](sleep/sleep-design.md) | `silver_sleep_garmin`, `silver_sleep_whoop`, `silver_sleep` | Specified (`SILVER-SLEEP`). The only two-source join; the other families are single-source reductions of the same shape |
| Glucose | [glucose/glucose-design.md](glucose/glucose-design.md) | `silver_glucose` | Specified (`SILVER-GLUCOSE`) |
| Cycles | [cycles/cycles-design.md](cycles/cycles-design.md) | `silver_recovery`, `silver_strain` | Specified (`SILVER-CYCLES`) |
| Workouts | — | `silver_workouts`, `silver_workout_splits`, `silver_whoop_workouts` | *(not yet specified; see `docs/SILVER.md`)* |
| Daily | — | `silver_daily`, `silver_body`, `silver_fitness` | *(not yet specified; see `docs/SILVER.md`)* |
| Weather | — | `silver_weather` | *(not yet specified; see `docs/SILVER.md`)* |

This parent doc owns no specs; each family's leaf owns its tables' specs.

## Configuration

| Variable | Meaning |
|---|---|
| `BRONZE_ROOT` | Required; bronze, read-only |
| `SILVER_ROOT` | Required; silver output, never under `BRONZE_ROOT` |
| `SILVER_MONITOR_DIR` | Optional, reserved for future silver-check state, outside `SILVER_ROOT` |
| `USCRN_TIMEZONE` | The USCRN station's IANA zone for weather's local day (default `America/New_York`) |

## References

- `docs/high-level-design.md`: layers are rebuildable; no transactional database; local-day
  semantics at read time
- `docs/DEPLOYMENT.md`: the silver container and the schedules to enable
