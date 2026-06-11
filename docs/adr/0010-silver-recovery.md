# ADR 0010: Silver recovery — per-cycle grain, dedup on cycle_id, joins to sleep

## Status
Accepted.

## Context
Recovery (Whoop) is the fourth silver table and the natural companion to sleep. The Whoop
recovery bronze is `{"records": [...]}`; each record is one cycle's recovery, carrying `cycle_id`
and `sleep_id` (profiled live: both 1:1 with the recovery — 176 recoveries = 176 Whoop sleeps),
UTC `created_at`/`updated_at` (Whoop rescores), and a nested `score`
(`recovery_score`, `resting_heart_rate`, `hrv_rmssd_milli`, `spo2_percentage`,
`skin_temp_celsius`, `user_calibrating`).

## Decision
- **Grain: one row per cycle** (`silver_recovery`), keyed on `cycle_id`. Per-cycle facts only;
  trends/correlations are gold.
- **Dedup on `cycle_id`**, keeping the latest `updated_at` (mirrors the Whoop-sleep rescore
  handling).
- **Carry both join keys.** `sleep_id` and `cycle_id` link to `silver_sleep_whoop`'s
  `whoop_sleep_id` / `whoop_cycle_id`, so gold can attach recovery to the specific sleep (and via
  that, to the sleep's true local night).
- **`recovery_date` = UTC date of `created_at`.** Recovery records carry **no `timezone_offset`**,
  so unlike sleep we can't derive a local night from the recovery alone; the UTC date of
  `created_at` (≈ the wake morning) is a stable standalone date, and the precise alignment comes
  from the sleep join. Documented so it isn't mistaken for a local night.
- **Columns:** the five score metrics + `user_calibrating` (true on Whoop's calibration days,
  surfaced rather than dropped). All null-safe.
- **Same code location/image**, reading `whoop/recovery` via DuckDB; lineage on
  `whoop_bronze_recovery` by `AssetKey`.
- **Checks:** uniqueness on `cycle_id` + non-null key (ERROR), value ranges (ERROR), coverage vs
  bronze distinct `cycle_id` (WARN). Daily rebuild; off the `*_api` pools.

## Consequences
- ~176 recoveries today (2025-12-18 →), one Parquet at
  `{SILVER_ROOT}/recovery/silver_recovery.parquet`.
- Completes the Whoop physiological picture (sleep + recovery) and sets up the obvious gold joins:
  recovery vs the night's sleep, vs next-day glucose, vs prior-day workout load.

## Related
[[0001-bronze-only]], [[0003-token-file]], [[0007-silver-sleep]], [[0008-silver-glucose]],
[[0009-silver-workouts]]. Layer guide: `docs/SILVER.md`.
