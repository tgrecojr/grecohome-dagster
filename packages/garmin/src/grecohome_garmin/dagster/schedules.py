"""Jobs and schedules for the Garmin code location.

Two jobs (daily-partitioned collections, and the unpartitioned reference
collections), each using the **in-process executor** so all of a run's ops share
one logged-in Garmin client. Two daily schedules:

- ``garmin_daily`` **re-captures** the trailing ``lookback_days`` *completed*
  partitions on every tick, so each day is captured on ~``lookback_days``
  consecutive days. This is intentional: Garmin syncs late and restates days after
  the fact, and silver keeps the latest capture. Identical re-captures cost an API
  call but no storage (skip-identical, see ``pull.py``). Do not make this
  capture-once -- see ``docs/intent/garmin/garmin-design.md``.
- ``garmin_reference`` refreshes the reference/snapshot collections once a day.
"""

from collections.abc import Iterator

from dagster import (
    RunRequest,
    ScheduleEvaluationContext,
    define_asset_job,
    in_process_executor,
    schedule,
)

from grecohome_core.dagster.helpers import trailing_partition_keys
from grecohome_garmin.config import settings
from grecohome_garmin.dagster.assets import DAILY_ASSETS, GARMIN_DAILY, REFERENCE_ASSETS

garmin_daily_job = define_asset_job(
    "garmin_daily_job", selection=DAILY_ASSETS, executor_def=in_process_executor
)
garmin_reference_job = define_asset_job(
    "garmin_reference_job", selection=REFERENCE_ASSETS, executor_def=in_process_executor
)


# @spec GARMIN-SCHED-001, GARMIN-SCHED-002, GARMIN-SCHED-003, GARMIN-SCHED-004
@schedule(cron_schedule="0 7 * * *", job=garmin_daily_job, execution_timezone="UTC")
def garmin_daily(context: ScheduleEvaluationContext) -> Iterator[RunRequest]:
    """Daily: re-capture every trailing completed partition, every tick."""
    now = context.scheduled_execution_time
    for key in trailing_partition_keys(GARMIN_DAILY, now, settings.lookback_days):
        # run_key carries the tick so re-emitting a partition on a later tick is a
        # distinct run (Dagster dedupes schedule run keys only within one tick).
        yield RunRequest(run_key=f"{key}-{now:%Y%m%dT%H}", partition_key=key)


# @spec GARMIN-SCHED-005
@schedule(cron_schedule="0 7 * * *", job=garmin_reference_job, execution_timezone="UTC")
def garmin_reference(context: ScheduleEvaluationContext) -> RunRequest:
    """Daily: refresh the reference/snapshot collections."""
    return RunRequest(run_key=f"garmin-reference-{context.scheduled_execution_time:%Y%m%d}")
