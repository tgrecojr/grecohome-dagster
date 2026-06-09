"""Schedule + job for the USCRN code location.

One schedule re-materializes the trailing ``uscrn_lookback_days`` daily partitions
a few times a day (default every 6h). ``run_key`` includes the tick so each re-emit
is a distinct run; storage stays flat via content-hash dedup at capture (a finished
day's rows are identical across ticks). Older history is reachable via
``dagster backfill`` over the same asset.
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

uscrn_capture_job = define_asset_job("uscrn_capture_job", selection=[uscrn_bronze_hourly])


@schedule(cron_schedule="0 */6 * * *", job=uscrn_capture_job, execution_timezone="UTC")
def uscrn_schedule(context: ScheduleEvaluationContext) -> Iterator[RunRequest]:
    """Every 6h: re-capture the trailing daily partitions for the station."""
    now = context.scheduled_execution_time
    for key in trailing_partition_keys(SOIL_PARTITIONS, now, settings.uscrn_lookback_days):
        # run_key carries the tick so re-emitting a partition is a distinct run;
        # content-hash dedup at capture keeps storage flat.
        yield RunRequest(run_key=f"{key}-{now:%Y%m%dT%H}", partition_key=key)
