# Run monitors — EARS specs

Design: [monitors-design.md](monitors-design.md). Prefix `GOLD-MONITORS`. Facets: `FAILURE`
(`run_failure_monitor`), `QUEUE` (`run_queue_monitor`).

## Run failures

- [x] **GOLD-MONITORS-FAILURE-001**: When any run in any code location ends in failure, the `run_failure_monitor` sensor shall log one `dagster_run_failed` line at ERROR level carrying `job`, `location` (null when the run has no code location), `run_id` and `partition` (null when the run has none).
- [x] **GOLD-MONITORS-FAILURE-002**: The `dagster_run_failed` line shall carry no error message, stack trace or run configuration.
- [x] **GOLD-MONITORS-FAILURE-003**: The `run_failure_monitor` sensor shall have a default status of running and shall never launch a run.

## Run queue

- [x] **GOLD-MONITORS-QUEUE-001**: When the `run_queue_monitor` sensor evaluates, at most once per 60 seconds, and the instance has more than 15 queued runs, it shall log `run_queue_backed_up` at WARNING level with `queued` and `threshold`, and in every case return a skip reason stating whether the queue is backed up or healthy.
- [x] **GOLD-MONITORS-QUEUE-002**: The `run_queue_monitor` sensor shall have a default status of running and shall never launch a run.
