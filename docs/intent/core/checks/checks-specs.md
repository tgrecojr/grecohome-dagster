# Bronze checks — EARS specs

Design: [checks-design.md](checks-design.md). Prefix `CORE-CHECKS`. Facets: none (config,
building, general behavior), `FRESH`, `COMPLETE`, `SCHEMA`, `CONTENT`, `ALERT`, `JOB`.

## Configuration and building

- [x] **CORE-CHECKS-001**: If a `CollectionCheckConfig` has `event_date_source="payload"` without an `event_date_field`, or names a family outside `freshness`, `completeness`, `schema` and `content`, then the system shall reject it when it is constructed.
- [x] **CORE-CHECKS-002**: When `build_collection_checks` builds checks for a config, the system shall build one asset check per family in `enabled_checks`, named `<source>_<collection>_<family>` (`freshness`, `completeness`, `schema_drift`, `content_health`) and attached to the config's `asset_key`, except that no completeness check is built when `event_date_source` is `none`.
- [x] **CORE-CHECKS-003**: The bronze checks shall read only the `recent_partitions` most recent existing `dt=` folders of a collection, and all of them when `recent_partitions` is 0 or less.
- [x] **CORE-CHECKS-004**: The bronze checks shall exclude `*.meta.json` sidecars from every payload read, and shall compute schema signatures from one exact payload file, so no sidecar field ever appears in a signature.
- [x] **CORE-CHECKS-005**: The system shall build every bronze check as non-blocking (`blocking=False`), so a failed check never aborts its asset's materialization.
- [x] **CORE-CHECKS-006**: If a bronze check raises internally, then the system shall return a failed result instead of raising, at ERROR severity for freshness and schema drift and at WARN severity for completeness and content health.

## Freshness

- [x] **CORE-CHECKS-FRESH-001**: When the bronze freshness check runs, it shall fail at ERROR severity if the newest sidecar fetch time across the collection's trailing folders is older than `cadence_hours + grace_hours`, and pass otherwise.
- [x] **CORE-CHECKS-FRESH-002**: If a collection has no sidecars in its trailing folders when the bronze freshness check runs, then the check shall pass when the config is `expected_empty` and fail at ERROR severity otherwise.

## Completeness

- [x] **CORE-CHECKS-COMPLETE-001**: When the bronze completeness check runs, it shall fail at WARN severity if two consecutive distinct event dates are more than `cadence_days` apart, and pass otherwise.
- [x] **CORE-CHECKS-COMPLETE-002**: The bronze completeness check shall take event dates from the `dt=` folders when `event_date_source` is `partition`, and from each record's `event_date_field` (JSON) or column (CSV) when it is `payload`, ignoring values that don't parse.
- [x] **CORE-CHECKS-COMPLETE-003**: If fewer than two distinct event dates are found, then the bronze completeness check shall pass.

## Schema drift

- [x] **CORE-CHECKS-SCHEMA-001**: The bronze schema-drift check shall compute the collection's signature from the richest of each trailing folder's newest payload (the most top-level tokens, ties to the newer), ignoring empty JSON and CSV payloads.
- [x] **CORE-CHECKS-SCHEMA-002**: When the bronze schema-drift check finds no baseline, or an empty one, under `BRONZE_MONITOR_DIR`, it shall record the current signature as the baseline and pass with status `baseline_set`.
- [x] **CORE-CHECKS-SCHEMA-003**: If the current signature differs from the recorded baseline, then the bronze schema-drift check shall fail at ERROR severity with status `drift` and the `was` and `now` signatures.
- [x] **CORE-CHECKS-SCHEMA-004**: If `BRONZE_MONITOR_DIR` is unset, then the bronze schema-drift check shall pass with status `disabled` and write nothing; if the baseline path would resolve inside `BRONZE_ROOT`, then it shall refuse to write and fail at ERROR severity.
- [x] **CORE-CHECKS-SCHEMA-005**: If no non-empty payload exists in the trailing folders, then the bronze schema-drift check shall pass with status `no_payload`.

## Content health

- [x] **CORE-CHECKS-CONTENT-001**: When the bronze content-health check runs, it shall sample up to 5 payloads spread across the collection's trailing folders.
- [x] **CORE-CHECKS-CONTENT-002**: If any sampled payload has an integrity issue (sha256 or byte size disagreeing with the sidecar, a missing or unreadable sidecar, a zero-byte payload, or JSON that doesn't parse), then the bronze content-health check shall fail at ERROR severity, including for an `expected_empty` collection.
- [x] **CORE-CHECKS-CONTENT-003**: If any sampled payload classifies as empty, error-like, an HTTP error or unparseable, then the bronze content-health check shall fail at WARN severity, except that empty classes pass when the config is `expected_empty`.
- [x] **CORE-CHECKS-CONTENT-004**: If no payload can be sampled, then the bronze content-health check shall pass.

## Alerting

- [x] **CORE-CHECKS-ALERT-001**: When an asset check wrapped by `alerting_check` returns a failing result at ERROR severity, the system shall log one `asset_check_failed` line at ERROR level with the check name and asset; a passing or WARN result shall log nothing.
- [x] **CORE-CHECKS-ALERT-002**: If logging the `asset_check_failed` line fails, then `alerting_check` shall swallow the error and return the check's result unchanged.
- [x] **CORE-CHECKS-ALERT-003**: The system shall wrap the bronze freshness, schema-drift and content-health checks with `alerting_check`.
- [x] **CORE-CHECKS-ALERT-004**: If an asset check wrapped by `alerting_check` raises, then the system shall log one `asset_check_failed` line at ERROR level with the check name and asset, and re-raise the exception.

## Checks-only job and schedule

- [x] **CORE-CHECKS-JOB-001**: When `build_bronze_checks_job` builds a job from asset checks, the job shall run those checks without materializing their assets, and use no concurrency pool.
- [x] **CORE-CHECKS-JOB-002**: When `build_bronze_checks_schedule` builds a schedule for a checks-only job, the schedule shall run hourly at minute 0 UTC unless another cron is given, be named `<job>_schedule` unless named explicitly, and start running.
