# ADR 0009: Silver workouts — per-activity grain, dedup on activityId

## Status
Accepted.

## Context
Workouts (Garmin activities) is the third silver table and the second single-source one. The
Garmin activities bronze is a top-level JSON array of activity objects per file; Garmin appends
and re-fetches overlapping windows, so the same activity recurs across many files (753 raw
elements → 727 distinct, profiled live). Each activity carries a local `startTimeLocal` and a UTC
`startTimeGMT`.

## Decision
- **Grain: one row per activity** (`silver_workouts`), keyed on `activityId`. Per-activity facts
  only; weekly/monthly training-load rollups are analysis and belong in gold.
- **Dedup on `activityId`**, keeping the latest fetch (`fetched_ms` from the bronze filename) —
  activities are immutable but re-captured, and an edit should take the newest.
- **Event date = local date of `startTimeLocal`.** Garmin already supplies local time, so unlike
  Whoop sleep there is no timezone derivation; both `start_time_local` and `start_time_gmt` are
  kept as TIMESTAMPs.
- **Columns:** identity + type (`activityType.typeKey`), the three durations, `distance_m`,
  `calories`, `avg_hr`/`max_hr`, per-zone `hr_z1..5_sec`, `device_id`. All metric fields are
  extracted null-safe — `distance` is absent on non-distance sports, and `averageHR` is `0` (not
  null) when HR wasn't recorded, so the range check **allows 0**.
- **Same code location/image**, reading `garmin/activities` via DuckDB; lineage on
  `garmin_bronze_activities` by `AssetKey`. Reuses the JSON payload reader (the file is a JSON
  array, unnested directly).
- **Checks:** uniqueness on `activityId` + non-null key (ERROR), value ranges (ERROR), coverage
  vs bronze distinct `activityId` (WARN). Daily rebuild; off the `*_api` pools.

## Consequences
- ~727 activities today (2022-07-16 → 2025-12-30), one Parquet at
  `{SILVER_ROOT}/workouts/silver_workouts.parquet`.
- Activities are intermittent (no daily cadence); gaps are normal, so coverage is WARN-only.
- Joinable to `silver_sleep` / `silver_glucose` by local date in gold (e.g. training load vs next
  day's recovery or glucose response).

## Related
[[0001-bronze-only]], [[0004-garmin-port]], [[0007-silver-sleep]], [[0008-silver-glucose]].
Layer guide: `docs/SILVER.md`.
