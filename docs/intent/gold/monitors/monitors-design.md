---
parent: gold
prefix: GOLD-MONITORS
---

# Run monitors

## Context

Self-hosted Dagster has no push alerting, and two failure modes are invisible to asset checks:

- **a failed run**: the asset keeps its last good output, every check on it still passes, and
  only a freshness check notices, hours or days later;
- **a backed-up run queue**: a run holding a concurrency pool, with the hourly schedules piling up
  behind it.

Two sensors watch the **whole Dagster instance** for these and emit structured log lines that the
Loki → Grafana → Slack path pages on (HLD: *Failures page through Loki and Grafana to Slack*).
They ship in the gold code location, the top of the stack and the lightest image; a sensor there
sees every code location's runs.

Both are **observers**: they never launch runs. Both **start running**, like every safety net (HLD).

## `run_failure_monitor`

A Dagster run-failure sensor that monitors **every code location**. For each run that ends in
**failure** (not cancellation), it logs one `dagster_run_failed` line at ERROR level carrying:

- `job`: the job name;
- `location`: the code location, or null for a run launched outside one (the CLI, for example);
- `run_id`;
- `partition`: the run's partition key, or null (an unpartitioned run, or a range backfill run as
  one).

A run the daemon terminates for exceeding the maximum runtime is marked failed, so it pages. On
its first evaluation the sensor only records where it is and alerts on nothing older; after daemon
downtime it catches up from where it stopped.

It carries no error text, so a credential or payload fragment in an exception message can never
reach Loki through this line.

A Grafana rule turns the line into a Slack page. The rule's query and threshold are set in Grafana
(see *Alert rule*).

## `run_queue_monitor`

A sensor evaluated at most every **60 seconds**. It counts the instance's **queued** runs; when the
count is **above 15**, it logs `run_queue_backed_up` (with `queued` and `threshold`) at WARNING
level. It always returns a skip reason ("Run queue backed up: …" or "Run queue healthy: …").

15 sits above a normal top-of-hour burst, when several schedules emit trailing-window runs that
drain through the per-source pools in minutes. The Grafana rule's 30-minute `for:` means only a
sustained backup pages.

## Alert rule

| Rule | Fires on | For |
|---|---|---|
| Dagster run failing | any `dagster_run_failed` line | immediately (evaluated each minute) |
| Dagster run queue backed up | `run_queue_backed_up` | 30 m |

Rules live in Grafana, not in this repo; `docs/DEPLOYMENT.md` lists them.

## Decisions & Alternatives

| Decision | Chosen | Alternatives Considered | Rationale |
|---|---|---|---|
| Failed-run alerting | A run-failure sensor logging `dagster_run_failed` | A Grafana rule on Dagster's own `RUN_FAILURE` log text; a daily failed-run digest | The sensor is code, specified and tested, and independent of Dagster's log wording. A digest would page the next day. |
| Where the sensors live | The gold code location | A dedicated monitoring code location | Gold is the lightest image; another location means another image to build and run. |
| Default status | Running | Stopped, enabled by hand | A safety net that depends on someone switching it on can silently never run. |
| What a failure line carries | Job, location, run id, partition | The error message | Exception text can carry credentials or payload fragments; the run id leads to the full error in Dagster. |
| Cancelled runs | Not alerted | Alert on any non-success | A cancellation is deliberate. |

## Open Questions & Future Decisions

### Deferred

1. **Monitoring the monitors.** If the gold code location fails to load, neither sensor runs; if
   the instance query raises, that tick errors in Dagster without a log line. Either way nothing
   pages. A Grafana rule on the absence of the gold location's logs would cover both.

## References

- `packages/gold/src/grecohome_gold/dagster/monitoring.py`
