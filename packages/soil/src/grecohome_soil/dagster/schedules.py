"""Schedules + job for the USCRN code location.

Two schedules drive the one capture job:

* ``uscrn_schedule`` (every 6h) re-materializes the trailing ``uscrn_lookback_days``
  daily partitions, one run each -- the live feed, catching the UTC-midnight
  rollover and rows that post during the day.
* ``uscrn_correction_schedule`` (weekly) re-captures the trailing
  ``uscrn_correction_lookback_days`` partitions in **one partition-range run**.
  NOAA rewrites the year files in place long after the fact (gap hours recovered
  from the station logger, corrupted rows repaired -- seen up to two years late),
  and only a re-slice picks that up. The asset fetches each year file once per
  run, and content-hash dedup at capture means an unchanged day writes nothing,
  so the sweep is cheap. Anything older than that window needs a manual backfill.

``run_key`` includes the tick so each re-emit is a distinct run.
"""

from collections.abc import Iterator

from dagster import (
    RunRequest,
    ScheduleEvaluationContext,
    define_asset_job,
    schedule,
)

from grecohome_core.dagster.helpers import trailing_partition_keys
from grecohome_soil.config import settings
from grecohome_soil.dagster.assets import SOIL_PARTITIONS, uscrn_bronze_hourly

# Dagster's documented run tags for materializing a contiguous partition range in
# one run (the asset reads them back via ``context.partition_keys``).
PARTITION_RANGE_START_TAG = "dagster/asset_partition_range_start"
PARTITION_RANGE_END_TAG = "dagster/asset_partition_range_end"

uscrn_capture_job = define_asset_job("uscrn_capture_job", selection=[uscrn_bronze_hourly])


@schedule(cron_schedule="0 */6 * * *", job=uscrn_capture_job, execution_timezone="UTC")
def uscrn_schedule(context: ScheduleEvaluationContext) -> Iterator[RunRequest]:
    """Every 6h: re-capture the trailing daily partitions for the station."""
    now = context.scheduled_execution_time
    for key in trailing_partition_keys(SOIL_PARTITIONS, now, settings.uscrn_lookback_days):
        # run_key carries the tick so re-emitting a partition is a distinct run;
        # content-hash dedup at capture keeps storage flat.
        yield RunRequest(run_key=f"{key}-{now:%Y%m%dT%H}", partition_key=key)


@schedule(
    cron_schedule="30 3 * * 0",  # Sunday 03:30 UTC, clear of the 6-hourly ticks
    job=uscrn_capture_job,
    execution_timezone="UTC",
)
def uscrn_correction_schedule(context: ScheduleEvaluationContext) -> Iterator[RunRequest]:
    """Weekly: re-slice a long trailing window in one run to pick up NOAA's late
    back-corrections of the year files."""
    now = context.scheduled_execution_time
    keys = trailing_partition_keys(SOIL_PARTITIONS, now, settings.uscrn_correction_lookback_days)
    yield RunRequest(
        run_key=f"correction-{keys[0]}-{keys[-1]}-{now:%Y%m%dT%H}",
        tags={PARTITION_RANGE_START_TAG: keys[0], PARTITION_RANGE_END_TAG: keys[-1]},
    )
