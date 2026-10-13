# Silver workouts — EARS specs

Design: [workouts-design.md](workouts-design.md). Prefix `SILVER-WORKOUTS`. Facets: `GARMIN`
(`silver_workouts`), `SPLITS` (`silver_workout_splits`), `WHOOP` (`silver_whoop_workouts`),
`CHECKS`, `SCHED`.

## Garmin activities table

- [x] **SILVER-WORKOUTS-GARMIN-001**: When `silver_workouts` materializes, the system shall read every element of every `garmin/activities` bronze payload's top-level array and write `{SILVER_ROOT}/workouts/silver_workouts.parquet` with one row per `activity_id` holding the `silver_workouts` columns of the workouts design's schema.
- [x] **SILVER-WORKOUTS-GARMIN-002**: If a `garmin/activities` bronze payload is not a JSON array, then the `silver_workouts` rebuild shall fail, leaving the existing table unchanged.
- [x] **SILVER-WORKOUTS-GARMIN-003**: When several captures hold the same `activityId`, `silver_workouts` shall keep the one with the latest fetch time in its bronze filename.
- [x] **SILVER-WORKOUTS-GARMIN-004**: If a Garmin activity's `activityId` is missing or JSON null, then `silver_workouts` shall leave it out; if it is present but not numeric, then `silver_workouts` shall keep the activity with `activity_id` null.
- [x] **SILVER-WORKOUTS-GARMIN-005**: The `silver_workouts` table shall set `activity_date` to the date of the activity's `startTimeLocal` (its first 10 characters).
- [x] **SILVER-WORKOUTS-GARMIN-006**: When `silver_workouts` materializes, the system shall record `rows`, `bronze_files` and `path` as materialization metadata.

## Garmin laps table

- [x] **SILVER-WORKOUTS-SPLITS-001**: When `silver_workout_splits` materializes, the system shall take each activity's laps from the `lapDTOs` array of that activity's latest `garmin/activity_splits` capture that has a `lapDTOs` array (by fetch time in the bronze filename; a capture whose `lapDTOs` is missing or null is ignored, and one whose `lapDTOs` is empty supplies no laps), and write `{SILVER_ROOT}/workout_splits/silver_workout_splits.parquet` with one row per (`activity_id`, `lap_index`) holding the `silver_workout_splits` columns of the workouts design's schema.
- [x] **SILVER-WORKOUTS-SPLITS-002**: If a `garmin/activity_splits` bronze payload's `lapDTOs` is present but not an array, then the `silver_workout_splits` rebuild shall fail, leaving the existing table unchanged.
- [x] **SILVER-WORKOUTS-SPLITS-003**: If a lap's payload `activityId` or its `lapIndex` is missing or JSON null, then `silver_workout_splits` shall leave the lap out; if either is present but not numeric, then `silver_workout_splits` shall keep the lap with that key null.
- [x] **SILVER-WORKOUTS-SPLITS-004**: When `silver_workout_splits` materializes, the system shall record `rows`, `bronze_files` and `path` as materialization metadata.

## Whoop workouts table

- [x] **SILVER-WORKOUTS-WHOOP-001**: When `silver_whoop_workouts` materializes, the system shall read every element of the `records` array of every `whoop/workout` bronze payload, skipping payloads without `records`, and write `{SILVER_ROOT}/whoop_workouts/silver_whoop_workouts.parquet` with one row per `workout_id` holding the `silver_whoop_workouts` columns of the workouts design's schema.
- [x] **SILVER-WORKOUTS-WHOOP-002**: If a `whoop/workout` bronze payload's `records` is present but not an array, then the `silver_whoop_workouts` rebuild shall fail, leaving the existing table unchanged.
- [x] **SILVER-WORKOUTS-WHOOP-003**: When several captures hold the same Whoop workout `id`, `silver_whoop_workouts` shall keep the one with the latest `updated_at`.
- [x] **SILVER-WORKOUTS-WHOOP-004**: If a Whoop workout record's `id` is missing or JSON null, then `silver_whoop_workouts` shall leave it out.
- [x] **SILVER-WORKOUTS-WHOOP-005**: The `silver_whoop_workouts` table shall set `workout_date` to the date of the record's UTC `start` shifted by its `timezone_offset` (`±HH:MM`, the minutes taking the hours' sign), and to the UTC date of `start` when the offset is missing or unparseable.
- [x] **SILVER-WORKOUTS-WHOOP-006**: When `silver_whoop_workouts` materializes, the system shall record `rows`, `bronze_files` and `path` as materialization metadata.

## Asset checks

- [x] **SILVER-WORKOUTS-CHECKS-001**: The `workouts_id_unique_nonnull` check shall fail at ERROR severity when `activity_id` has a duplicate or a null, or `activity_date` has a null.
- [x] **SILVER-WORKOUTS-CHECKS-002**: The `workouts_value_ranges` check shall fail at ERROR severity when any non-null `duration_sec`, `moving_duration_sec`, `elapsed_duration_sec`, `distance_m` or `calories` is below 0, `duration_sec` is at least 604,800 (7 days), `distance_m` is at least 1,000,000 (1,000 km), or `avg_hr` or `max_hr` is outside 0–240.
- [x] **SILVER-WORKOUTS-CHECKS-003**: The `workouts_coverage_vs_bronze` check shall fail at WARN severity when `silver_workouts`' row count is less than the number of distinct numeric `activityId` values in `garmin/activities` bronze.
- [x] **SILVER-WORKOUTS-CHECKS-004**: The `splits_lap_unique_nonnull` check shall fail at ERROR severity when (`activity_id`, `lap_index`) has a duplicate or either is null.
- [x] **SILVER-WORKOUTS-CHECKS-005**: The `splits_value_ranges` check shall fail at ERROR severity when any non-null `duration_sec`, `distance_m` or `avg_speed_mps` is below 0, `avg_hr` or `max_hr` is outside 0–240, or `lap_index` is below 1.
- [x] **SILVER-WORKOUTS-CHECKS-006**: The `splits_coverage_vs_bronze` check shall fail at WARN severity when `silver_workout_splits`' row count is less than the number of distinct numeric (`activityId`, `lapIndex`) pairs in the captures `silver_workout_splits` selects (each activity's latest capture that has a `lapDTOs` array), and shall report the number of distinct activities.
- [x] **SILVER-WORKOUTS-CHECKS-007**: The `whoop_workout_unique_nonnull` check shall fail at ERROR severity when `workout_id` has a duplicate or a null, or `workout_date` has a null.
- [x] **SILVER-WORKOUTS-CHECKS-008**: The `whoop_workout_value_ranges` check shall fail at ERROR severity when any non-null `strain` is outside 0–21, `avg_heart_rate` or `max_heart_rate` is outside 20–240, or `kilojoules` or `distance_m` is below 0.
- [x] **SILVER-WORKOUTS-CHECKS-009**: The `whoop_workout_coverage_vs_bronze` check shall fail at WARN severity when `silver_whoop_workouts`' row count is less than the number of distinct `id` values in `whoop/workout` bronze, and shall report the number of distinct `sport_name` values.
- [x] **SILVER-WORKOUTS-CHECKS-010**: The `silver_workouts_freshness`, `silver_workout_splits_freshness` and `silver_whoop_workouts_freshness` checks shall each fail at ERROR severity when their table's Parquet file was last modified more than 36 hours before the check runs.
- [x] **SILVER-WORKOUTS-CHECKS-011**: If a workouts table's Parquet doesn't exist, then every check on that table shall fail at ERROR severity.
- [x] **SILVER-WORKOUTS-CHECKS-012**: The system shall wrap every workouts, splits and Whoop workouts check in `alerting_check`.

## Schedule

- [x] **SILVER-WORKOUTS-SCHED-001**: The system shall define `silver_workouts_job`, `silver_workout_splits_job` and `silver_whoop_workouts_job`, each over its table and that table's asset checks, with daily schedules `silver_workouts_daily` at 06:45 UTC, `silver_workout_splits_daily` at 06:47 UTC and `silver_whoop_workouts_daily` at 06:53 UTC, each with a default status of stopped.
