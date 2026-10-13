# Silver glucose — EARS specs

Design: [glucose-design.md](glucose-design.md). Prefix `SILVER-GLUCOSE`. Facets: `TABLE`
(`silver_glucose`), `CHECKS`, `SCHED`.

## Table

- [x] **SILVER-GLUCOSE-TABLE-001**: When `silver_glucose` materializes, the system shall read every `lingo/glucose` bronze CSV by header name and write `{SILVER_ROOT}/glucose/silver_glucose.parquet` with one row per reading holding `reading_ts_utc`, `reading_ts_local`, `reading_date`, `tz_offset_minutes` and `mgdl`.
- [x] **SILVER-GLUCOSE-TABLE-002**: The `silver_glucose` table shall take `reading_ts_local` from the reading time's first 16 characters, `reading_date` from its first 10, `tz_offset_minutes` from its `±HH:MM` offset with the minutes taking the hours' sign, and `reading_ts_utc` as `reading_ts_local` minus that offset.
- [x] **SILVER-GLUCOSE-TABLE-003**: If a reading's local time doesn't parse, then `silver_glucose` shall leave the reading out; if its local time parses but its offset doesn't, then `silver_glucose` shall keep it with `reading_ts_utc` and `tz_offset_minutes` null.
- [x] **SILVER-GLUCOSE-TABLE-004**: When several bronze rows share a `reading_ts_utc`, `silver_glucose` shall keep the one from the capture with the latest fetch time in its bronze filename, for every column.
- [x] **SILVER-GLUCOSE-TABLE-005**: The `silver_glucose` table shall cast the measurement to an integer `mgdl`, keeping the reading with `mgdl` null when the measurement is missing or non-numeric.
- [x] **SILVER-GLUCOSE-TABLE-006**: When `silver_glucose` materializes, the system shall record `rows`, `bronze_files` and `path` as materialization metadata.

## Asset checks

- [x] **SILVER-GLUCOSE-CHECKS-001**: The `glucose_reading_unique_nonnull` check shall fail at ERROR severity when `reading_ts_utc` has a duplicate or a null, or `reading_date` has a null.
- [x] **SILVER-GLUCOSE-CHECKS-002**: The `glucose_value_range` check shall fail at ERROR severity when any non-null `mgdl` is below 10 or above 600.
- [x] **SILVER-GLUCOSE-CHECKS-003**: The `glucose_coverage_vs_bronze` check shall fail at WARN severity when the table's row count is less than the number of distinct parseable UTC instants in `lingo/glucose` bronze, and shall report the number of distinct `reading_date` values.
- [x] **SILVER-GLUCOSE-CHECKS-004**: The `silver_glucose_freshness` check shall fail at ERROR severity when the table's Parquet file was last modified more than 36 hours before the check runs.
- [x] **SILVER-GLUCOSE-CHECKS-005**: If the `silver_glucose` Parquet doesn't exist, then every glucose check shall fail at ERROR severity.
- [x] **SILVER-GLUCOSE-CHECKS-006**: The system shall wrap every glucose check in `alerting_check`.

## Schedule

- [x] **SILVER-GLUCOSE-SCHED-001**: The system shall define `silver_glucose_job` over `silver_glucose` and its asset checks, and a `silver_glucose_daily` schedule that runs it daily at 06:30 UTC with a default status of stopped.
