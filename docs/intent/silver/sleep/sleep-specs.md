# Silver sleep — EARS specs

Design: [sleep-design.md](sleep-design.md). Prefix `SILVER-SLEEP`. Facets: `GARMIN`
(`silver_sleep_garmin`), `WHOOP` (`silver_sleep_whoop`), `JOIN` (`silver_sleep`), `CHECKS`,
`SCHED`.

## Garmin source table

- [x] **SILVER-SLEEP-GARMIN-001**: When `silver_sleep_garmin` materializes, the system shall read every `garmin/sleep` bronze payload and write `{SILVER_ROOT}/sleep/_garmin.parquet` with one row per `night_date`, taken from `dailySleepDTO.calendarDate`, holding the `garmin_*` columns of the sleep design's schema.
- [x] **SILVER-SLEEP-GARMIN-002**: When several Garmin sleep captures share a `night_date`, `silver_sleep_garmin` shall keep the one with the latest fetch time in its bronze filename.
- [x] **SILVER-SLEEP-GARMIN-003**: If a Garmin sleep payload is valid JSON but has no parseable `dailySleepDTO.calendarDate`, then `silver_sleep_garmin` shall leave it out.
- [x] **SILVER-SLEEP-GARMIN-004**: The `silver_sleep_garmin` table shall convert Garmin's stage seconds to minutes (÷ 60), and shall parse `sleepStartTimestampGMT` and `sleepEndTimestampGMT` as UTC timestamps from either epoch milliseconds or ISO strings, with any other value becoming `NULL`.
- [x] **SILVER-SLEEP-GARMIN-005**: When `silver_sleep_garmin` materializes, the system shall record `rows`, `path` and `bronze_files` as materialization metadata.

## Whoop source table

- [x] **SILVER-SLEEP-WHOOP-001**: When `silver_sleep_whoop` materializes, the system shall read every record in the `records` array of every `whoop/sleep` bronze payload and write `{SILVER_ROOT}/sleep/_whoop.parquet` with one row per Whoop sleep `id`, holding `night_date`, `whoop_sleep_id`, `is_nap` and the `whoop_*` columns of the sleep design's schema.
- [x] **SILVER-SLEEP-WHOOP-002**: The `silver_sleep_whoop` table shall set each record's `night_date` to the date of its UTC `end` shifted by its `timezone_offset` (`±HH:MM`, the minutes taking the hours' sign), and to the UTC date of `end` when the offset is missing or unparseable.
- [x] **SILVER-SLEEP-WHOOP-003**: When several captures hold the same Whoop sleep `id`, `silver_sleep_whoop` shall keep the one with the latest `updated_at`.
- [x] **SILVER-SLEEP-WHOOP-004**: If a Whoop sleep record has no `id` or no derivable `night_date`, then `silver_sleep_whoop` shall leave it out.
- [x] **SILVER-SLEEP-WHOOP-005**: The `silver_sleep_whoop` table shall set `is_nap` from the record's `nap`, defaulting to false, and shall convert Whoop's stage milliseconds to minutes (÷ 60000).
- [x] **SILVER-SLEEP-WHOOP-006**: When `silver_sleep_whoop` materializes, the system shall record `rows`, `path` and `bronze_files` as materialization metadata.

## Unified table

- [x] **SILVER-SLEEP-JOIN-001**: When `silver_sleep` materializes, the system shall write `{SILVER_ROOT}/sleep/silver_sleep.parquet` as a full outer join on `night_date` of `_garmin.parquet` and one non-nap `_whoop.parquet` record per night, with every `garmin_*` and `whoop_*` column kept as its source produced it (none coalesced) and `has_garmin` / `has_whoop` set to whether each side has the night.
- [x] **SILVER-SLEEP-JOIN-002**: When a night has more than one non-nap Whoop record, `silver_sleep` shall use the one with the latest `whoop_start`, ranking a missing `whoop_start` last.
- [x] **SILVER-SLEEP-JOIN-003**: When `silver_sleep` materializes, the system shall record `rows` and `path` as materialization metadata.

## Asset checks

- [x] **SILVER-SLEEP-CHECKS-001**: The `garmin_night_unique_nonnull` check on `silver_sleep_garmin` shall fail at ERROR severity when `night_date` has a duplicate or a `NULL`.
- [x] **SILVER-SLEEP-CHECKS-002**: The `whoop_id_unique_night_nonnull` check on `silver_sleep_whoop` shall fail at ERROR severity when `whoop_sleep_id` has a duplicate or `night_date` has a `NULL`.
- [x] **SILVER-SLEEP-CHECKS-003**: The `sleep_night_unique_nonnull` check on `silver_sleep` shall fail at ERROR severity when `night_date` has a duplicate or a `NULL`.
- [x] **SILVER-SLEEP-CHECKS-004**: The `sleep_value_ranges` check on `silver_sleep` shall fail at ERROR severity when `garmin_sleep_score` or any Whoop percentage column is below 0 or above 100, or when any stage-minute column is below 0 or at least 1440.
- [x] **SILVER-SLEEP-CHECKS-005**: The `sleep_join_sanity` check on `silver_sleep` shall fail at WARN severity when a row has neither source, and shall report the number of nights on or after 2025-12-18 that have only one source.
- [x] **SILVER-SLEEP-CHECKS-006**: The `sleep_coverage_split` check on `silver_sleep` shall always pass, reporting the counts of nights with both sources, Garmin only and Whoop only.
- [x] **SILVER-SLEEP-CHECKS-007**: The `garmin_coverage_vs_bronze` check on `silver_sleep_garmin` shall fail at WARN severity when the table's row count is less than the number of distinct parseable `dailySleepDTO.calendarDate` values in `garmin/sleep` bronze.
- [x] **SILVER-SLEEP-CHECKS-008**: If a sleep check's table Parquet doesn't exist, then that check shall fail at ERROR severity.
- [x] **SILVER-SLEEP-CHECKS-009**: The system shall wrap every sleep check in `alerting_check`, so any sleep check result that fails at ERROR severity alerts.
- [x] **SILVER-SLEEP-CHECKS-010**: The `silver_sleep_garmin_freshness`, `silver_sleep_whoop_freshness` and `silver_sleep_freshness` checks shall each fail at ERROR severity when their table's Parquet file was last modified more than 36 hours before the check runs.

## Schedule

- [x] **SILVER-SLEEP-SCHED-001**: The system shall define `silver_sleep_job` over `silver_sleep_garmin`, `silver_sleep_whoop`, `silver_sleep` and their asset checks, and a `silver_sleep_daily` schedule that runs it daily at 06:00 UTC with a default status of stopped.
