"""Jobs and schedules for the Garmin code location.

Two jobs (daily-partitioned collections, and the unpartitioned reference
collections), each using the **in-process executor** so all of a run's ops share
one logged-in Garmin client. Two daily schedules:

- ``garmin_daily`` re-materializes the trailing ``lookback_days`` *completed*
  partitions with ``run_key = partition_key`` -- so each day is captured **exactly
  once** ever (Dagster's run-key dedup catches up missed days without re-pulling).
  This is the no-dedup-compatible analogue of Whoop's hourly window.
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


@schedule(cron_schedule="0 7 * * *", job=garmin_daily_job, execution_timezone="UTC")
def garmin_daily(context: ScheduleEvaluationContext) -> Iterator[RunRequest]:
    """Daily: capture the trailing completed partitions, each exactly once."""
    now = context.scheduled_execution_time
    for key in trailing_partition_keys(GARMIN_DAILY, now, settings.lookback_days):
        # run_key == partition_key (no hour suffix): Dagster runs each partition at
        # most once ever, so immutable Garmin data is never re-captured/duplicated.
        yield RunRequest(run_key=key, partition_key=key)


@schedule(cron_schedule="0 7 * * *", job=garmin_reference_job, execution_timezone="UTC")
def garmin_reference(context: ScheduleEvaluationContext) -> RunRequest:
    """Daily: refresh the reference/snapshot collections."""
    return RunRequest(run_key=f"garmin-reference-{context.scheduled_execution_time:%Y%m%d}")
