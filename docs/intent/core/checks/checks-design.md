---
parent: core
prefix: CORE-CHECKS
---

# Check helpers

## Context and Design Philosophy

Silver and gold data quality runs inside Dagster as `@asset_check`s attached to each table's
asset. Failures show up in the UI next to materializations, page through Slack when they matter,
and catch the one failure a per-run check can't: **a table that stops being rebuilt.**

Core provides three table-agnostic helpers; each layer writes its own table checks and uses them:

- **`alerting_check`** turns a failing ERROR check into a page.
- **The checks-only job and schedule builders** run a code location's checks on a clock,
  independent of its rebuilds.
- **`table_freshness_check`** builds the ERROR check that notices a stopped rebuild.

**Severity is deliberate.** A broken table or a stopped rebuild is **ERROR** and pages. Coverage
and expectation drift is frequently legitimate (a device not worn, a sparse source), so it's
**WARN** and stays in the UI. A check that cries wolf gets ignored. Each layer's design says
which of its checks is which.

## Alerting

Dagster OSS has no push alerting: a failed check is a framework event in the instance's
storage, not a log line. The `alerting_check` decorator closes that gap. When a wrapped check
returns a **failing ERROR** result, it logs one structured `asset_check_failed` line at ERROR,
carrying only `check` (the name) and `asset`, through the shared logger, which ships to Loki. A
Grafana Loki rule on that line posts to Slack, so one rule covers every layer's page-worthy
checks. WARN failures aren't emitted; they stay in the UI and don't page.

- An error while logging is swallowed and never breaks the check.
- **A check that raises also pages.** If the wrapped check raises instead of returning a result,
  the decorator logs the same `asset_check_failed` line and re-raises, so the run still fails as
  it would unwrapped. A check that crashes on bad input (a corrupt file it reads, for example)
  is at least as page-worthy as one that fails cleanly.
- Silver and gold wrap every one of their checks, so a WARN check that hits an ERROR condition
  (its table's Parquet missing, for example) pages too.

## Checks-only job and schedule

A per-run check can't notice a table that stopped being rebuilt. So each code location also
wires a **checks-only job** (`build_checks_job`: `AssetSelection.checks(...)`, which runs the
checks without materializing their assets) and a **UTC schedule** for it (`build_checks_schedule`;
named `<job>_schedule` unless named explicitly). A job can't span code locations, so there's one
per code location.

**The job runs in-process** (`in_process_executor`): its check steps run one after another in
the run's own process. Under Dagster's default multiprocess executor each check step spawns a
fresh Python subprocess that imports Dagster and the code location before running a check that
is itself a sub-second local Parquet read. With dozens of checks in one job, that spawn-and-
import cost is nearly all of the job's CPU time, and it lands on every core the host has, at
once. In-process does the same work on one core, and because each check is a fraction of a
second the job's wall time stays in the same range. There is nothing to parallelize: checks
share no client or resource. The cost is that one hung check holds up the checks queued behind
it in that run, and those never report. The backstop is the host's run monitoring (see
`docs/DEPLOYMENT.md`), which ends a run that exceeds its limit; the ended run fails, and a
failed run pages through the run-failure monitor (HLD: *Failures page through Loki and Grafana
to Slack*), so a hang is not silent. That limit is far shorter than any checks cadence, so
consecutive checks runs never overlap. A check that raises fails only its own step; the other
steps still run (the in-process executor continues past a failed step with the steps that don't
depend on it).

**The schedule's cron is chosen per code location.** `build_checks_schedule` has no default
cron: the caller names the minute and the cadence. The builder accepts only a cron whose minute
field is one literal minute from 1 to 59 and rejects any other (minute `0`, `*`, a step or a
list) when the schedule is built, so the first rule below holds by construction. Two rules guide
the choice.

- **Never minute 0, and no two checks schedules at the same time.** The host runs its own
  cron jobs at the top of the hour, and every code location's checks job landing there as well
  is a CPU stampede on a shared host. Each checks schedule takes a minute and hour no other
  schedule uses. The times in use are listed in `docs/DEPLOYMENT.md` (*Schedules and sensors*).
- **Cadence matches what the checks can detect.** Table freshness fails only once a table is
  older than its limit, and the layer's checks run reports it at most one check interval after.
  Silver and gold rebuild daily, so they check daily, timed after the day's rebuilds. Checking
  more often shortens that interval and nothing else, at the cost of CPU. The cadence also sets
  how often a persisting ERROR repeats its `asset_check_failed` line (once per checks run);
  Grafana folds the repeats into one alert.

The schedule **starts running** (`default_status` RUNNING), so deploying a code location turns
its checks on with no manual step. A schedule someone deliberately stopped in the UI stays
stopped: Dagster keeps a schedule's state from the UI over its code default. A check that fails
on its very first run (before a table's first rebuild, for example) fails and pages like any
other; that is accepted, because it clears itself once rebuilds start. The job has **no
concurrency pool**: checks read local files only, so nothing they contend for needs a limit.

## Table freshness

Silver and gold tables are whole-table rebuilds written by the atomic Parquet writer, so a
table's age is its file's modification time. `table_freshness_check(asset, path_fn)` builds an
ERROR check named **`<asset>_freshness`** that:

- **fails when the Parquet at `path_fn()` was last modified more than 36 hours** before the check
  runs, and passes at 36 hours or less, reporting the age and the limit;
- **fails at ERROR when the file doesn't exist**;
- alerts through `alerting_check`;
- calls `path_fn` when the check runs, so a layer root is read at check time.

It measures rebuilds, not data: a rebuild over stale bronze still passes. Bronze freshness
belongs to WearStream's own checks. Why 36 hours: see the silver design, *Asset checks*.

## Decisions & Alternatives

| Decision | Chosen | Alternatives Considered | Rationale |
|---|---|---|---|
| Paging path | ERROR failures log `asset_check_failed` → Grafana Loki rule → Slack | Dagster+ alerting; email | Dagster OSS has no push alerts; logs already ship to Loki and Grafana already alerts from it. |
| A check that raises | Logs `asset_check_failed`, then re-raises | Pass the exception through unlogged; have each check catch its own errors | A raised check is only a failed step in the UI, which pages no one. Catching per check is easy to forget; one place in the decorator covers every check. |
| Stopped-rebuild detection | A checks-only job on its own schedule, plus a table-freshness check per table | Rely on per-materialization checks | Per-run checks never run for a table whose rebuild stopped or failed. |
| Checks job executor | In-process | Dagster's default multiprocess executor; multiprocess with a low `max_concurrent` | A check is a sub-second local read; a per-step subprocess spends far more CPU importing the code location than checking, across every core at once. Capping `max_concurrent` serializes the spawns but keeps every one of them. In-process does the work on one core in about the same wall time; checks share nothing, so parallelism bought nothing. |
| Checks schedule cron | A required argument: a distinct off-minute per code location, at a cadence matched to the layer's rebuilds | A shared hourly-at-`:00` default; a minute derived from the job name | A shared default would put every code location on one minute, and a new code location would inherit it by omission. A derived minute is invisible to a reader and can still collide. Naming the cron at each call site keeps the choice visible and makes each new code location decide. |
| Minute-0 guard | The builder rejects a cron whose minute field isn't one literal minute 1–59 | Guidance only; a lint | A rule that only lives in prose is the kind a new code location copies past. The guard costs a few lines and turns the stampede into a load-time error. Distinctness across code locations can't be checked in one builder, so that half stays guidance plus the deployment table. |
| Checks schedule default | Starts running | Starts stopped (Dagster's default), enabled by hand | A safety net that depends on someone remembering to switch it on can silently never run. A first-run failure that clears itself is an acceptable cost. |

## References

- `packages/core/src/grecohome_core/checks/` (`alerting.py`, `jobs.py`, `table_freshness.py`)
- `docs/intent/silver/checks/` and `docs/intent/gold/checks/`: each layer's checks job and
  schedule
