# Silver daily (daily summary, body, fitness) — EARS specs

Design: [daily-design.md](daily-design.md). Prefix `SILVER-DAILY`. Facets: `SUMMARY`
(`silver_daily`), `BODY` (`silver_body`), `FITNESS` (`silver_fitness`), `CHECKS`, `SCHED`.

## Daily summary table

- [x] **SILVER-DAILY-SUMMARY-001**: When `silver_daily` materializes, the system shall read every `garmin/user_summary` bronze payload and write `{SILVER_ROOT}/daily/silver_daily.parquet` with one row per `activity_date`, taken from `calendarDate`, holding the `silver_daily` columns of the daily design's schema.
- [x] **SILVER-DAILY-SUMMARY-002**: When several captures share an `activity_date`, `silver_daily` shall keep the one with the latest fetch time in its bronze filename.
- [x] **SILVER-DAILY-SUMMARY-003**: If a `garmin/user_summary` payload has no parseable `calendarDate`, then `silver_daily` shall leave it out.
- [x] **SILVER-DAILY-SUMMARY-004**: The `silver_daily` table shall set `avg_stress_level` and `max_stress_level` to null when Garmin reports `-1` or `-2`.
- [x] **SILVER-DAILY-SUMMARY-005**: When `silver_daily` materializes, the system shall record `rows`, `bronze_files` and `path` as materialization metadata.

## Body table

- [x] **SILVER-DAILY-BODY-001**: When `silver_body` materializes, the system shall read every element of the `dateWeightList` array of every `garmin/daily_weigh_ins` bronze payload, skipping payloads without `dateWeightList`, and write `{SILVER_ROOT}/body/silver_body.parquet` with one row per `sample_pk` holding the `silver_body` columns of the daily design's schema.
- [x] **SILVER-DAILY-BODY-002**: If a `garmin/daily_weigh_ins` bronze payload's `dateWeightList` is present but not an array, then the `silver_body` rebuild shall fail, leaving the existing table unchanged.
- [x] **SILVER-DAILY-BODY-003**: When several captures hold the same `samplePk`, `silver_body` shall keep the one with the latest fetch time in its bronze filename.
- [x] **SILVER-DAILY-BODY-004**: If a weigh-in's `samplePk` is missing or JSON null, then `silver_body` shall leave it out; if it is present but not numeric, then `silver_body` shall keep the weigh-in with `sample_pk` null.
- [x] **SILVER-DAILY-BODY-005**: The `silver_body` table shall convert `weight`, `boneMass`, `muscleMass` and `weightDelta` from grams to kilograms (÷ 1000), and set `measured_ts_utc` from the epoch-millisecond `timestampGMT`.
- [x] **SILVER-DAILY-BODY-006**: When `silver_body` materializes, the system shall record `rows`, `bronze_files` and `path` as materialization metadata.

## Fitness table

- [x] **SILVER-DAILY-FITNESS-001**: When `silver_fitness` materializes, the system shall write `{SILVER_ROOT}/fitness/silver_fitness.parquet` with one row per `snapshot_date` that appears in any of the VO2max, training-status or race-prediction relations, joining the three on that date and leaving a relation's columns null on dates it lacks.
- [x] **SILVER-DAILY-FITNESS-002**: The `silver_fitness` table shall take `vo2max_running` from a `garmin/max_metrics` capture's first element's `generic.vo2MaxValue` dated by `generic.calendarDate`, and `vo2max_cycling` from `cycling.vo2MaxValue` dated by `cycling.calendarDate`, using only captures that carry that value and keeping the latest such capture per sport and date.
- [x] **SILVER-DAILY-FITNESS-003**: The `silver_fitness` table shall date each entry of a `garmin/training_status` capture's `mostRecentTrainingStatus.latestTrainingStatusData` map by the entry's own `calendarDate`, and per date keep the entry whose `primaryTrainingDevice` is true over one whose is not, then the one from the latest capture, recording its `deviceId` as `training_device_id`.
- [x] **SILVER-DAILY-FITNESS-004**: The `silver_fitness` table shall date each `garmin/race_predictions` capture by its bronze `dt` folder and keep the latest capture per date.
- [x] **SILVER-DAILY-FITNESS-005**: When `silver_fitness` materializes, the system shall record `rows`, `bronze_files` (across the three collections) and `path` as materialization metadata.

## Asset checks

- [x] **SILVER-DAILY-CHECKS-001**: The `daily_date_unique_nonnull` check shall fail at ERROR severity when `activity_date` has a duplicate or a null.
- [x] **SILVER-DAILY-CHECKS-002**: The `daily_value_ranges` check shall fail at ERROR severity when any non-null value is out of range: `total_steps` 0–200,000, `total_distance_m` below 0, `active_kilocalories` 0–30,000, `resting_heart_rate` 20–150, `max_heart_rate` 20–240, `avg_stress_level` 0–100, `avg_spo2` 50–100, `body_battery_high` 0–100.
- [x] **SILVER-DAILY-CHECKS-003**: The `daily_coverage_vs_bronze` check shall fail at WARN severity when `silver_daily`'s row count is less than the number of distinct parseable `calendarDate` values in `garmin/user_summary` bronze, and shall report the number of days with steps.
- [x] **SILVER-DAILY-CHECKS-004**: The `body_sample_unique_nonnull` check shall fail at ERROR severity when `sample_pk` has a duplicate or a null, or `measured_date` has a null.
- [x] **SILVER-DAILY-CHECKS-005**: The `body_value_ranges` check shall fail at ERROR severity when any non-null value is out of range: `weight_kg` 20–300, `bmi` 10–80, `body_fat_pct` 0–70, `body_water_pct` 0–100, `muscle_mass_kg` 0–200.
- [x] **SILVER-DAILY-CHECKS-006**: The `body_coverage_vs_bronze` check shall fail at WARN severity when `silver_body`'s row count is less than the number of distinct numeric `samplePk` values in `garmin/daily_weigh_ins` bronze, and shall report the number of distinct `measured_date` values.
- [x] **SILVER-DAILY-CHECKS-007**: The `fitness_day_unique_nonnull` check shall fail at ERROR severity when `snapshot_date` has a duplicate or a null.
- [x] **SILVER-DAILY-CHECKS-008**: The `fitness_value_ranges` check shall fail at ERROR severity when any non-null `vo2max_running` or `vo2max_cycling` is outside 10–90, `weekly_training_load` is below 0, or `race_5k_sec` or `race_marathon_sec` is 0 or below.
- [x] **SILVER-DAILY-CHECKS-009**: The `fitness_coverage` check shall fail at WARN severity when `silver_fitness` has no rows, and shall report each metric's coverage.
- [x] **SILVER-DAILY-CHECKS-010**: The system shall define `silver_daily_freshness`, `silver_body_freshness` and `silver_fitness_freshness` with `table_freshness_check` on their tables.
- [x] **SILVER-DAILY-CHECKS-011**: If a daily-family table's Parquet doesn't exist, then every check on that table shall fail at ERROR severity.
- [x] **SILVER-DAILY-CHECKS-012**: The system shall wrap every daily, body and fitness check in `alerting_check`.

## Schedule

- [x] **SILVER-DAILY-SCHED-001**: The system shall define `silver_daily_job`, `silver_body_job` and `silver_fitness_job`, each over its table and that table's asset checks, with daily schedules `silver_daily_daily` at 06:40 UTC, `silver_body_daily` at 06:42 UTC and `silver_fitness_daily` at 06:44 UTC, each with a default status of stopped.
