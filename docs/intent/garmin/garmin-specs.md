# Garmin bronze subject — EARS specs

Design: [garmin-design.md](garmin-design.md). Prefix `GARMIN`. Facets: `SCHED` (capture
cadence), `CAPTURE` (bronze writes and skip-identical), `CHECKS` (bronze-check guarantees).

## Capture cadence

- [x] **GARMIN-SCHED-001**: When the `garmin_daily` schedule ticks (daily at 07:00 UTC), the system shall emit one run request for each of the trailing `LOOKBACK_DAYS` completed Garmin daily partitions.
- [x] **GARMIN-SCHED-002**: The `garmin_daily` schedule shall never emit a run request for the current, still-open Garmin daily partition.
- [x] **GARMIN-SCHED-003**: When the `garmin_daily` schedule ticks, the system shall emit a run request for every partition in the trailing window, including partitions already captured on earlier ticks, so that each completed day is re-captured on every tick while it remains in the window.
- [x] **GARMIN-SCHED-004**: The `garmin_daily` schedule shall give each run request a `run_key` of the form `<partition_key>-<YYYYMMDDTHH>`, where the suffix is the tick's scheduled execution time in UTC.
- [x] **GARMIN-SCHED-005**: When the `garmin_reference` schedule ticks (daily at 07:00 UTC), the system shall emit exactly one run request for the Garmin reference job, with a `run_key` of `garmin-reference-<YYYYMMDD>` for the tick's UTC date.
- [x] **GARMIN-SCHED-006**: Where the `LOOKBACK_DAYS` environment variable is unset, the system shall size the Garmin daily re-capture window (the `garmin_daily` trailing partitions) at 14 completed daily partitions.

## Bronze writes

- [x] **GARMIN-CAPTURE-001**: If a Garmin endpoint call returns `None`, then the system shall write no bronze capture for that request, for every endpoint regardless of its catalog flags.
- [x] **GARMIN-CAPTURE-002**: If a Garmin endpoint flagged `skip_if_empty` returns an empty list or dict, then the system shall write no bronze capture for that request.
- [x] **GARMIN-CAPTURE-003**: When a Garmin endpoint not flagged `skip_if_empty` returns an empty list or dict, the system shall pass it to the bronze writer as a faithful "asked, no data" capture, subject to the skip-identical rule in GARMIN-CAPTURE-004.
- [x] **GARMIN-CAPTURE-004**: When capturing a Garmin single-request endpoint (a `daily` or `range` endpoint, the `activities` list, or a catalog `static`, `weekly` or `per_profile` reference endpoint; never the per-activity fan-out) that is not flagged `skip_if_empty` or `skip_if_none`, the system shall skip writing a payload whose sha256 equals the newest capture's sha256 in that `(collection, dt)` bronze folder.
- [x] **GARMIN-CAPTURE-005**: When capturing a Garmin endpoint flagged `skip_if_empty` or `skip_if_none`, the system shall write every capture without the skip-identical check, whatever the endpoint's kind.
- [x] **GARMIN-CAPTURE-006**: When capturing the Garmin per-activity fan-out (each `PER_ACTIVITY` endpoint and each activity download, captured once per activity), the system shall write every capture without the skip-identical check.
- [x] **GARMIN-CAPTURE-007**: When capturing a Garmin `per_device` or `per_device_range` endpoint (one request per discovered device), the system shall write every capture without the skip-identical check.
- [x] **GARMIN-CAPTURE-008**: When capturing the Garmin `static_goals` endpoint (one request per goal status), the system shall write every capture without the skip-identical check.
- [x] **GARMIN-CAPTURE-009**: The system shall write each Garmin daily-partitioned asset's captures into the bronze folder `dt=<partition_key>`, and each Garmin reference asset's captures into the bronze folder `dt=<UTC fetch date>`.
- [x] **GARMIN-CAPTURE-010**: The system shall capture Garmin activity downloads byte-for-byte with `capture_mode` `raw`, and every other Garmin return as deterministic compact JSON with `capture_mode` `reserialized`.
- [x] **GARMIN-CAPTURE-011**: Where a Garmin endpoint is flagged `screen_secrets`, the system shall remove secret-named keys from the payload before capture and record the removed key paths in the sidecar's `redacted_fields`.

## Bronze-check guarantees

- [x] **GARMIN-CHECKS-001**: The system shall configure a fetch-freshness check for every selected Garmin collection, with a tolerance of 26 h cadence plus 6 h grace.
- [x] **GARMIN-CHECKS-002**: When a Garmin single-request endpoint under the skip-identical rule (GARMIN-CAPTURE-004) is captured into a `(collection, dt)` bronze folder that holds no prior capture, the system shall write the capture, so that each newly completed partition and each new reference fetch-date gives the freshness check a new sidecar.
