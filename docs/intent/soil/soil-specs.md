# Soil (NOAA USCRN) bronze subject — EARS specs

Design: [soil-design.md](soil-design.md). Prefix `SOIL`. Facets: `CAPTURE` (fetch, slice,
capture), `SCHED` (schedules), `CHECKS` (the `uscrn/hourly` check config).

## Capture

- [x] **SOIL-CAPTURE-001**: When the USCRN asset materializes a daily partition `YYYYMMDD`, the system shall select, from each year file that can hold that date, the lines whose second whitespace-separated field equals `YYYYMMDD`, in file order, skipping blank lines and lines with fewer than two fields, and keep each selected line's content unchanged.
- [x] **SOIL-CAPTURE-002**: When the USCRN asset materializes the Jan 1 partition, the system shall read the previous year's file before that year's file, so the `0000` row (published as the previous year's last row) is captured and the day is in hour order.
- [x] **SOIL-CAPTURE-003**: When a USCRN partition has at least one selected row, the system shall capture the rows, joined with `\n` and ending with a newline, as `text/plain` with `ext=txt` and skip-identical on, to `uscrn/hourly/dt=<partition>`.
- [x] **SOIL-CAPTURE-004**: If a USCRN partition has no selected rows, then the system shall write nothing for it.
- [x] **SOIL-CAPTURE-005**: The system shall record, in each USCRN capture's sidecar, `request_url` (the partition's own year file) and `request_params` holding `station`, `wbanno` (from the first row), `year`, `year_files` (the files consulted), `utc_date` and `row_count`, and no credentials.
- [x] **SOIL-CAPTURE-006**: If a USCRN year file returns HTTP 404, then the system shall treat it as having no rows, log a `uscrn year file not found` WARNING, and continue the run.
- [x] **SOIL-CAPTURE-007**: If a USCRN year-file request fails with any HTTP error other than 404, then the system shall raise and fail the run.
- [x] **SOIL-CAPTURE-008**: While a USCRN run covers several partitions, the system shall fetch each year file at most once, caching a 404 as well.
- [x] **SOIL-CAPTURE-009**: When a USCRN run finishes, the system shall record `partitions`, `first_partition`, `last_partition`, `rows`, `captured` and `year_files` (each year fetched, `ok` or `not found (404)`) as materialization metadata.
- [x] **SOIL-CAPTURE-010**: The system shall split UI and CLI backfills of the USCRN asset into runs of at most 366 partitions.

## Schedules

- [x] **SOIL-SCHED-001**: When the `uscrn_schedule` schedule ticks (every 6 hours at minute 0 UTC), the system shall emit one run request for each of the trailing `USCRN_LOOKBACK_DAYS` daily partitions, including the current UTC day, with a `run_key` of `<partition>-<YYYYMMDDTHH>`.
- [x] **SOIL-SCHED-002**: When the `uscrn_correction_schedule` schedule ticks (Sundays 03:30 UTC), the system shall emit exactly one run request covering the trailing `USCRN_CORRECTION_LOOKBACK_DAYS` daily partitions as a single partition range, with a `run_key` of `correction-<first>-<last>-<YYYYMMDDTHH>`.
- [x] **SOIL-SCHED-003**: The system shall default `USCRN_LOOKBACK_DAYS` to 2 and `USCRN_CORRECTION_LOOKBACK_DAYS` to 400, so the weekly correction run spans the current and the previous year file.

## Bronze checks

- [x] **SOIL-CHECKS-001**: The system shall configure the `uscrn/hourly` bronze checks with the text reader, the partition as the event date, a freshness tolerance of 30 h cadence plus 12 h grace, a completeness gap tolerance of 2 days, and all four check families, run hourly at minute 43 UTC by `uscrn_bronze_checks_job`.
