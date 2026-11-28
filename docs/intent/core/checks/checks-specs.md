# Check helpers — EARS specs

Design: [checks-design.md](checks-design.md). Prefix `CORE-CHECKS`. Facets: `ALERT`
(`alerting_check`), `JOB` (the checks-only job and schedule builders), `TABLE` (table freshness).

## Alerting

- [x] **CORE-CHECKS-ALERT-001**: When an asset check wrapped by `alerting_check` returns a failing result at ERROR severity, the system shall log one `asset_check_failed` line at ERROR level with the check name and asset; a passing or WARN result shall log nothing.
- [x] **CORE-CHECKS-ALERT-002**: If logging the `asset_check_failed` line fails, then `alerting_check` shall swallow the error and return the check's result unchanged.
- [x] **CORE-CHECKS-ALERT-004**: If an asset check wrapped by `alerting_check` raises, then the system shall log one `asset_check_failed` line at ERROR level with the check name and asset, and re-raise the exception.

## Checks-only job and schedule

- [x] **CORE-CHECKS-JOB-001**: When `build_checks_job` builds a job from asset checks, the job shall run those checks without materializing their assets, and use no concurrency pool.
- [x] **CORE-CHECKS-JOB-002**: When `build_checks_schedule` builds a schedule for a checks-only job from the cron it is given, the schedule shall run on that cron in UTC, be named `<job>_schedule` unless named explicitly, and start running.
- [x] **CORE-CHECKS-JOB-003**: When `build_checks_job` builds a job from asset checks, the job shall use Dagster's in-process executor, so that its check steps run one after another in the run's own process.
- [x] **CORE-CHECKS-JOB-004**: The `cron_schedule` argument of `build_checks_schedule` shall be a required keyword argument with no default value.
- [x] **CORE-CHECKS-JOB-005**: If the first whitespace-separated field of the cron given to `build_checks_schedule` is not a decimal integer from 1 to 59, then `build_checks_schedule` shall raise `ValueError` when it is called, before any schedule is built.

## Table freshness

- [x] **CORE-CHECKS-TABLE-001**: When a check built by `table_freshness_check(asset, path_fn)` runs, it shall be named `<asset>_freshness`, pass when the Parquet at `path_fn()` was last modified at most 36 hours earlier, and otherwise fail, at ERROR severity in both cases, reporting the age and the limit.
- [x] **CORE-CHECKS-TABLE-002**: If the Parquet at `path_fn()` doesn't exist, then a check built by `table_freshness_check` shall fail at ERROR severity.
- [x] **CORE-CHECKS-TABLE-003**: The system shall wrap every check built by `table_freshness_check` in `alerting_check`.
