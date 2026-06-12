# ADR 0011: Gold layer — daily wellness mart as the spine

## Status
Accepted.

## Context
With the four silver tables complete (sleep, glucose, workouts, recovery), the next layer is gold:
analysis marts. Silver deliberately deferred aggregation and cross-source joins ("one faithful row
per event"); gold is where those live. We need a first deliverable and a layer shape.

## Decision
- **Gold is its own code location / image** (`packages/gold`, `grecohome_gold`), mirroring the
  layer-per-image pattern (bronze subjects, silver). It reads `SILVER_ROOT` via DuckDB and writes
  marts under a new `GOLD_ROOT`; depends only on `grecohome-core` + DuckDB. Silver upstreams are
  declared by `AssetKey` (cross-location lineage); reads are filesystem reads, not gRPC.
- **First mart: `gold_daily_wellness`** — the spine. **One row per local day**, over a
  **continuous date spine** (gaps explicit, enabling rolling/streak analysis), left-joining:
  sleep (1:1 on `night_date`), recovery (deduped to one/day by latest `created_at`), workouts
  (aggregated by `activity_date`), glucose (aggregated by `reading_date`). Per-day `has_*`
  provenance flags.
- **Glucose time-in-range = 70–140 mg/dL** (non-diabetic / metabolic-health band), with explicit
  below/above split. Thresholds are constants in the mart, easy to revisit.
- **Recovery dedup-per-day.** Recovery's UTC `recovery_date` is occasionally 2-per-date; the mart
  keeps the latest `created_at`. The precise per-sleep linkage stays in `silver_recovery`.
- **Rebuildable, outside silver.** Whole-table overwrite each run; the atomic writer refuses any
  path under `SILVER_ROOT` (`write_parquet_atomic` guard generalized from `bronze_root` to
  `protected_root`).
- **Runs after silver** (07:30 UTC, post silver rebuild + checks). Checks: day uniqueness +
  non-null (ERROR), aggregate value ranges (ERROR), per-source coverage (WARN). Off the `*_api`
  pools.
- **`GoldSettings`** carries only `silver_root` + `gold_root` (+ reserved `gold_monitor_dir`) — no
  bronze, so it does not extend `BaseSubjectSettings`.

## Consequences
- ~1,467 daily rows today; the spine other analyses (device agreement, recovery-vs-load,
  glucose-vs-sleep) build on or beside.
- A fifth-plus published image (`grecohome-dagster-gold`) wired into the CI matrix and
  `workspace.yaml`; a new `GOLD_ROOT` mount.
- Daily aggregates (TIR, training load) now have a home; per-event detail stays in silver,
  joinable by date / `sleep_id` / `cycle_id`.

## Related
[[0007-silver-sleep]], [[0008-silver-glucose]], [[0009-silver-workouts]], [[0010-silver-recovery]].
Layer guide: `docs/GOLD.md`.
