"""Schedules and jobs for the Whoop code location.

One hourly schedule re-materializes the trailing ``reconcile_window_days + 1``
daily bronze partitions, so Whoop's retroactive rescores/deletes are eventually
re-captured (bronze just appends + content-hash dedups). A second hourly schedule
captures the current-only snapshots. Relaxed from the old 15-minute poll to hourly;
the trailing window, not the cadence, is what guarantees correctness.

Both schedules fire at ``:17``, not the top of the hour -- see ``_WHOOP_CRON``.
"""

from collections.abc import Iterator

from dagster import (
    RunRequest,
    ScheduleEvaluationContext,
    define_asset_job,
    schedule,
)

from grecohome_core.dagster.helpers import trailing_partition_keys
from grecohome_whoop.config import settings
from grecohome_whoop.dagster.assets import RANGE_ASSETS, WHOOP_DAILY, bronze_snapshots

# Daily-partitioned job over the four range collections (partitioning is inferred
# from the selected assets, which all share WHOOP_DAILY).
whoop_bronze_job = define_asset_job("whoop_bronze_job", selection=RANGE_ASSETS)

# Unpartitioned snapshots job (profile + body measurement).
whoop_snapshots_job = define_asset_job(
    "whoop_snapshots_job",
    selection=[bronze_snapshots],
)

# Run at :17, NOT the top of the hour. Our poll cadence (1h) equals the Whoop
# access-token lifetime (1h), so every tick refreshes the OAuth token right at its
# expiry cliff. Doing that at :00 lands the refresh in the internet-wide top-of-hour
# cron surge, when Whoop's /oauth/oauth2/token endpoint is most prone to be slow or
# return a 502 -- and a 502 mid-rotation loses the rotated single-use refresh token
# and revokes the grant (every observed grant-death fired at HH:00; see
# docs/WHOOP_TOKEN_RUNBOOK.md). Refreshing off the top of the hour dodges that
# congestion window. Both schedules share the minute so the cross-process token lock
# still serializes the single shared refresh they trigger.
_WHOOP_CRON = "17 * * * *"


@schedule(cron_schedule=_WHOOP_CRON, job=whoop_bronze_job, execution_timezone="UTC")
def whoop_hourly(context: ScheduleEvaluationContext) -> Iterator[RunRequest]:
    """Hourly: re-materialize the trailing daily bronze partitions."""
    now = context.scheduled_execution_time
    count = settings.reconcile_window_days + 1  # 7-day overlap + the settle partition
    for key in trailing_partition_keys(WHOOP_DAILY, now, count):
        # run_key includes the hour so re-emitting a partition each tick is a
        # distinct run; storage stays flat via content-hash dedup at capture.
        yield RunRequest(run_key=f"{key}-{now:%Y%m%dT%H}", partition_key=key)


@schedule(cron_schedule=_WHOOP_CRON, job=whoop_snapshots_job, execution_timezone="UTC")
def whoop_snapshots_hourly(context: ScheduleEvaluationContext) -> RunRequest:
    """Hourly: capture the current-only Whoop snapshots."""
    return RunRequest(run_key=f"snapshots-{context.scheduled_execution_time:%Y%m%dT%H}")
