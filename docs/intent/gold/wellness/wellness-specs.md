# Gold daily wellness — EARS specs

Design: [wellness-design.md](wellness-design.md). Prefix `GOLD-WELLNESS`. Facets: `MART`,
`CHECKS`, `SCHED`.

## Mart

- [x] **GOLD-WELLNESS-MART-001**: When `gold_daily_wellness` materializes, the system shall write `{GOLD_ROOT}/wellness/daily_wellness.parquet` with one row per day from the earliest to the latest date found in `silver_sleep.night_date`, `silver_recovery.recovery_date`, `silver_workouts.activity_date`, `silver_whoop_workouts.workout_date`, `silver_glucose.reading_date`, `silver_daily.activity_date` and `silver_strain.strain_date`.
- [x] **GOLD-WELLNESS-MART-002**: If a silver table the mart reads has no Parquet, then `gold_daily_wellness` shall treat it as an empty table with its expected columns and types, leaving that source's columns null.
- [x] **GOLD-WELLNESS-MART-003**: The `gold_daily_wellness` mart shall take each day's sleep columns from `silver_sleep` on `night_date`, its daily-summary columns from `silver_daily` on `activity_date`, and its recovery from the `silver_recovery` row with that `recovery_date` and the latest `created_at`.
- [x] **GOLD-WELLNESS-MART-004**: The `gold_daily_wellness` mart shall take each day's strain from the `silver_strain` row whose `cycle_id` equals that day's kept recovery's `cycle_id`, converting kilojoules to kilocalories (÷ 4.184).
- [x] **GOLD-WELLNESS-MART-005**: The `gold_daily_wellness` mart shall aggregate each day's Garmin activities from `silver_workouts` by `activity_date` into `workout_count` (0 when none), `workout_total_min`, `workout_distance_km` and `workout_calories`.
- [x] **GOLD-WELLNESS-MART-006**: The `gold_daily_wellness` mart shall aggregate each day's Whoop workouts from `silver_whoop_workouts` by `workout_date` into `whoop_workout_count` (0 when none), `whoop_workout_min` (the sum of `end_ts` − `start_ts`) and `whoop_workout_strain` (the day's highest `strain`).
- [x] **GOLD-WELLNESS-MART-007**: The `gold_daily_wellness` mart shall aggregate each day's non-null glucose readings by `reading_date` into `glucose_readings`, `glucose_mean`, `glucose_min`, `glucose_max`, `glucose_std` (sample), `glucose_tir_pct` (the percentage within 70–140 mg/dL inclusive), `glucose_pct_below` (< 70) and `glucose_pct_above` (> 140).
- [x] **GOLD-WELLNESS-MART-008**: The `gold_daily_wellness` mart shall give each day the weight, BMI and body fat of the latest weigh-in on or before that day, taking the later `measured_ts_utc` when a date has several weigh-ins, and present weight in kilograms and in pounds (× 2.20462).
- [x] **GOLD-WELLNESS-MART-009**: The `gold_daily_wellness` mart shall set `has_sleep`, `has_recovery`, `has_strain`, `has_daily`, `has_workout`, `has_whoop_workout`, `has_glucose` and `has_weight` to whether that source contributed a row to the day.
- [x] **GOLD-WELLNESS-MART-010**: When `gold_daily_wellness` materializes, the system shall record `rows`, `tir_range` and `path` as materialization metadata.

## Asset checks

- [x] **GOLD-WELLNESS-CHECKS-001**: The `wellness_day_unique_nonnull` check shall fail at ERROR severity when `day` has a duplicate or a null.
- [x] **GOLD-WELLNESS-CHECKS-002**: The `wellness_value_ranges` check shall fail at ERROR severity when any non-null value is out of range: `glucose_tir_pct` 0–100, `glucose_mean` 10–600, `recovery_score` 0–100, `workout_count` below 0, `workout_total_min` below 0, `glucose_readings` 0 or below, `day_strain` 0–21, `strain_kilocalories` below 0, `steps` 0–200,000, `active_calories` below 0, `avg_stress` 0–100, `weight_lb` 44–660, `body_fat_pct` 0–70.
- [x] **GOLD-WELLNESS-CHECKS-003**: The `wellness_coverage` check shall fail at WARN severity when no day has any source, and shall report the number of days and each source's day count.
- [x] **GOLD-WELLNESS-CHECKS-004**: The system shall define `gold_daily_wellness_freshness` with `table_freshness_check` on `gold_daily_wellness`.
- [x] **GOLD-WELLNESS-CHECKS-005**: If the `gold_daily_wellness` Parquet doesn't exist, then every wellness check shall fail at ERROR severity.
- [x] **GOLD-WELLNESS-CHECKS-006**: The system shall wrap every wellness check in `alerting_check`.

## Schedule

- [x] **GOLD-WELLNESS-SCHED-001**: The system shall define `gold_wellness_job` over `gold_daily_wellness` and its asset checks, and a `gold_wellness_daily` schedule that runs it daily at 07:30 UTC with a default status of stopped.
