# Silver cycles (recovery and strain) — EARS specs

Design: [cycles-design.md](cycles-design.md). Prefix `SILVER-CYCLES`. Facets: `RECOVERY`
(`silver_recovery`), `STRAIN` (`silver_strain`), `CHECKS`, `SCHED`.

## Recovery table

- [x] **SILVER-CYCLES-RECOVERY-001**: When `silver_recovery` materializes, the system shall read every element of the `records` array of every `whoop/recovery` bronze payload, skipping payloads without `records`, and write `{SILVER_ROOT}/recovery/silver_recovery.parquet` with one row per `cycle_id` holding the recovery columns of the cycles design's schema.
- [x] **SILVER-CYCLES-RECOVERY-002**: The `silver_recovery` table shall set `recovery_date` to the UTC date of the record's `created_at`.
- [x] **SILVER-CYCLES-RECOVERY-003**: When several captures hold the same recovery `cycle_id`, `silver_recovery` shall keep the one with the latest `updated_at`.
- [x] **SILVER-CYCLES-RECOVERY-004**: If a Whoop recovery record's `cycle_id` is missing or JSON null, then `silver_recovery` shall leave it out; if it is present but not numeric, then `silver_recovery` shall keep the record with `cycle_id` null.
- [x] **SILVER-CYCLES-RECOVERY-005**: The `silver_recovery` table shall set `user_calibrating` from `score.user_calibrating`, defaulting to false when absent.
- [x] **SILVER-CYCLES-RECOVERY-006**: The `silver_recovery` table shall carry each record's `score_state` as reported.
- [x] **SILVER-CYCLES-RECOVERY-007**: When `silver_recovery` materializes, the system shall record `rows`, `bronze_files` and `path` as materialization metadata.
- [x] **SILVER-CYCLES-RECOVERY-008**: If a `whoop/recovery` bronze payload's `records` is present but not an array, then the `silver_recovery` rebuild shall fail, leaving the existing table unchanged.

## Strain table

- [x] **SILVER-CYCLES-STRAIN-001**: When `silver_strain` materializes, the system shall read every element of the `records` array of every `whoop/cycle` bronze payload, skipping payloads without `records`, and write `{SILVER_ROOT}/strain/silver_strain.parquet` with one row per `cycle_id` (the record's `id`) holding the strain columns of the cycles design's schema.
- [x] **SILVER-CYCLES-STRAIN-002**: The `silver_strain` table shall set `strain_date` to the date of the record's UTC `start` shifted by its `timezone_offset` (`±HH:MM`, the minutes taking the hours' sign), and to the UTC date of `start` when the offset is missing or unparseable.
- [x] **SILVER-CYCLES-STRAIN-003**: When several captures hold the same strain `cycle_id`, `silver_strain` shall keep the one with the latest `updated_at`.
- [x] **SILVER-CYCLES-STRAIN-004**: If a Whoop cycle record's `id` is missing or JSON null, then `silver_strain` shall leave it out; if it is present but not numeric, then `silver_strain` shall keep the record with `cycle_id` null.
- [x] **SILVER-CYCLES-STRAIN-005**: The `silver_strain` table shall keep a cycle whose `end` or `score` is absent, with `end_ts` or the score metrics null, and shall carry `score_state` and `step_count` as reported.
- [x] **SILVER-CYCLES-STRAIN-006**: When `silver_strain` materializes, the system shall record `rows`, `bronze_files` and `path` as materialization metadata.
- [x] **SILVER-CYCLES-STRAIN-007**: If a `whoop/cycle` bronze payload's `records` is present but not an array, then the `silver_strain` rebuild shall fail, leaving the existing table unchanged.

## Asset checks

- [x] **SILVER-CYCLES-CHECKS-001**: The `recovery_cycle_unique_nonnull` check shall fail at ERROR severity when `silver_recovery.cycle_id` has a duplicate or a null, or `recovery_date` has a null.
- [x] **SILVER-CYCLES-CHECKS-002**: The `recovery_value_ranges` check shall fail at ERROR severity when any non-null value is out of range: `recovery_score` 0–100, `resting_heart_rate` 20–120, `hrv_rmssd_milli` 0–500, `spo2_percentage` 50–100, `skin_temp_celsius` 20–45.
- [x] **SILVER-CYCLES-CHECKS-003**: The `recovery_coverage_vs_bronze` check shall fail at WARN severity when `silver_recovery`'s row count is less than the number of distinct numeric `cycle_id` values in `whoop/recovery` bronze.
- [x] **SILVER-CYCLES-CHECKS-004**: The `strain_cycle_unique_nonnull` check shall fail at ERROR severity when `silver_strain.cycle_id` has a duplicate or a null, or `strain_date` has a null.
- [x] **SILVER-CYCLES-CHECKS-005**: The `strain_value_ranges` check shall fail at ERROR severity when any non-null value is out of range: `day_strain` 0–21, `avg_heart_rate` and `max_heart_rate` 20–240, `kilojoules` below 0, `step_count` below 0.
- [x] **SILVER-CYCLES-CHECKS-006**: The `strain_coverage_vs_bronze` check shall fail at WARN severity when `silver_strain`'s row count is less than the number of distinct numeric `id` values in `whoop/cycle` bronze, and shall report the number of distinct `strain_date` values.
- [x] **SILVER-CYCLES-CHECKS-007**: The `silver_recovery_freshness` and `silver_strain_freshness` checks shall each fail at ERROR severity when their table's Parquet file was last modified more than 36 hours before the check runs.
- [x] **SILVER-CYCLES-CHECKS-008**: If a cycles table's Parquet doesn't exist, then every check on that table shall fail at ERROR severity.
- [x] **SILVER-CYCLES-CHECKS-009**: The system shall wrap every recovery and strain check in `alerting_check`.

## Schedule

- [x] **SILVER-CYCLES-SCHED-001**: The system shall define `silver_recovery_job` over `silver_recovery` and its asset checks with a `silver_recovery_daily` schedule daily at 06:50 UTC, and `silver_strain_job` over `silver_strain` and its asset checks with a `silver_strain_daily` schedule daily at 06:52 UTC, each with a default status of stopped.
