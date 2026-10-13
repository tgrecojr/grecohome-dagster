# Silver layer checks — EARS specs

Design: [checks-design.md](checks-design.md). Prefix `SILVER-CHECKS`. Facets: `JOB`
(the checks-only job and schedule), `FRESH` (the table freshness builder).

## Checks-only job

- [x] **SILVER-CHECKS-JOB-001**: The system shall define `silver_checks_job` to run the asset checks of every silver table, from every family's checks list, without materializing any asset.
- [x] **SILVER-CHECKS-JOB-002**: The system shall define a `silver_checks_daily` schedule that runs `silver_checks_job` daily at 07:00 UTC with a default status of running.

## Table freshness

- [x] **SILVER-CHECKS-FRESH-001**: When a check built by `table_freshness_check(asset, path_fn)` runs, it shall be named `<asset>_freshness`, pass when the Parquet at `path_fn()` was last modified at most 36 hours earlier, and otherwise fail, at ERROR severity in both cases, reporting the age and the limit.
- [x] **SILVER-CHECKS-FRESH-002**: If the Parquet at `path_fn()` doesn't exist, then a check built by `table_freshness_check` shall fail at ERROR severity.
- [x] **SILVER-CHECKS-FRESH-003**: The system shall wrap every check built by `table_freshness_check` in `alerting_check`.
