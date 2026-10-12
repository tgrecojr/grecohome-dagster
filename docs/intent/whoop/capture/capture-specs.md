# Whoop capture — EARS specs

Design: [capture-design.md](capture-design.md). Prefix `WHOOP-CAPTURE`. Facets: `SCHED`
(schedules), none (capture), `PAGE` (pagination), `CHECKS` (bronze collection checks).

## Schedules

- [x] **WHOOP-CAPTURE-SCHED-001**: When the `whoop_hourly` schedule ticks, the system shall emit one run request for each of the trailing `RECONCILE_WINDOW_DAYS + 1` Whoop daily partitions, including the current, in-progress UTC day.
- [x] **WHOOP-CAPTURE-SCHED-002**: The `whoop_hourly` schedule shall give each run request a `run_key` of the form `<partition_key>-<YYYYMMDDTHH>`, where the suffix is the tick's scheduled execution time in UTC.
- [x] **WHOOP-CAPTURE-SCHED-003**: When the `whoop_snapshots_hourly` schedule ticks, the system shall emit exactly one run request with a `run_key` of `snapshots-<YYYYMMDDTHH>`.
- [x] **WHOOP-CAPTURE-SCHED-004**: Both Whoop API schedules (`whoop_hourly` and `whoop_snapshots_hourly`) shall run hourly at minute 17 UTC, never at minute 0.

## Capture

- [x] **WHOOP-CAPTURE-001**: When a Whoop range-collection asset (`sleep`, `recovery`, `workout` or `cycle`) materializes a daily partition, the system shall request that collection with `start` set to the partition's 00:00 UTC and `end` set to the next day's 00:00 UTC, both formatted `YYYY-MM-DDTHH:MM:SS.mmmZ`.
- [x] **WHOOP-CAPTURE-002**: When a Whoop range-collection request returns a successful page containing at least one record, the system shall capture that page's response bytes to `whoop/<collection>/dt=<partition_key>` with skip-identical on.
- [x] **WHOOP-CAPTURE-003**: If a Whoop range-collection page contains zero records, then the system shall not capture it.
- [x] **WHOOP-CAPTURE-004**: If a Whoop API request (range page or snapshot) returns an HTTP error status, then the system shall not capture the response body.
- [x] **WHOOP-CAPTURE-005**: If a Whoop API request returns a 2xx response whose body is not valid JSON, then the system shall raise a non-retryable `WhoopAPIError` without capturing the body.
- [x] **WHOOP-CAPTURE-006**: When a Whoop range-collection asset finishes a partition, the system shall record the number of records fetched as `records` materialization metadata, together with the `partition`.
- [x] **WHOOP-CAPTURE-007**: When the Whoop snapshots asset runs, the system shall fetch the profile and then the body measurement, capturing each response into the bronze folder for the UTC fetch date with skip-identical on.
- [x] **WHOOP-CAPTURE-008**: If a Whoop data request (range page or snapshot) fails with a timeout, a network error, HTTP 429 or HTTP 500/502/503/504, then the system shall retry that request, up to 3 attempts per request. A failure to obtain the access token is not a data-request failure and is never retried (WHOOP-AUTH-009).
- [x] **WHOOP-CAPTURE-009**: Where a retryable Whoop data-request response carries a `Retry-After` header in seconds, the system shall wait that long before the next attempt, capped at 60 seconds.
- [x] **WHOOP-CAPTURE-010**: If a Whoop data request returns an HTTP error status other than 429 or 500/502/503/504, then the system shall raise `WhoopAPIError` without retrying.
- [x] **WHOOP-CAPTURE-011**: The Whoop API client shall acquire its rate limiter (sized from `MAX_REQUESTS_PER_MINUTE`) before every data-request attempt, retries included.

## Pagination

- [x] **WHOOP-CAPTURE-PAGE-001**: The system shall request Whoop range-collection pages with a `limit` of at most 25, and send each response's `next_token` as the `nextToken` parameter of the next request.
- [x] **WHOOP-CAPTURE-PAGE-002**: When a Whoop range-collection response has no `next_token`, or contains zero records, the system shall end pagination for that request window.
- [x] **WHOOP-CAPTURE-PAGE-003**: If a Whoop range-collection response carries a `next_token` already seen in the same pagination, then the system shall stop paginating, log a WARNING and return the records fetched so far.
- [x] **WHOOP-CAPTURE-PAGE-004**: If a Whoop range-collection pagination has fetched 1000 pages, then the system shall stop paginating, log a WARNING and return the records fetched so far.

## Bronze collection checks

- [x] **WHOOP-CAPTURE-CHECKS-001**: The system shall configure, for each of the Whoop `sleep`, `recovery` and `cycle` collections, a freshness check (26 h cadence plus 6 h grace), a completeness check (event-date gap over 2 days, with the event date from record `start` for `sleep` and `cycle` and from `created_at` for `recovery`), a schema-drift check and a content-health check.
- [x] **WHOOP-CAPTURE-CHECKS-002**: The system shall configure, for the Whoop `workout` collection, only a completeness check (event-date gap over 14 days, event date from record `start`) and a schema-drift check.
- [x] **WHOOP-CAPTURE-CHECKS-003**: The system shall configure, for each of the Whoop `profile` and `body_measurement` snapshots, a freshness check (26 h cadence plus 6 h grace), a schema-drift check and a content-health check, and no completeness check.
- [x] **WHOOP-CAPTURE-CHECKS-004**: The system shall run the Whoop bronze collection checks and the token-health check (WHOOP-AUTH-HEALTH) hourly at minute 0 UTC in `whoop_bronze_checks_job`, outside the `whoop_api` concurrency pool.
