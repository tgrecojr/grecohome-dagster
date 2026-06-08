"""Reusable Dagster building blocks shared across data subjects.

- :func:`daily_utc_partitions` — the standard daily, UTC-sliced partition set.
- :func:`trailing_partition_keys` — the trailing window a schedule re-captures.
- :func:`run_async` — bridge async fetch code into a sync Dagster op/asset.
"""

import asyncio
from collections.abc import Awaitable, Sequence
from datetime import datetime, timedelta

from dagster import DailyPartitionsDefinition, TimeWindowPartitionsDefinition


def daily_utc_partitions(start_date: str) -> DailyPartitionsDefinition:
    """Daily partitions in UTC.

    Each partition is a **UTC fetch-slice** ``[day 00:00, next day 00:00)``, not a
    semantic local day. Local-day ("day"/"night") semantics belong in downstream
    silver/gold, applied at read time over bronze's raw UTC timestamps.

    Args:
        start_date: Earliest partition date, ``"YYYY-MM-DD"`` (the backfill floor).
    """
    return DailyPartitionsDefinition(start_date=start_date, timezone="UTC")


def trailing_partition_keys(
    partitions_def: TimeWindowPartitionsDefinition,
    before: datetime,
    count: int,
) -> list[str]:
    """Return the trailing ``count`` daily partition keys ending with ``before``'s day.

    The partition that ``before`` falls into (the in-progress current day) is
    **included** so the hourly schedule re-captures intraday data; advancing the
    clock one day makes Dagster treat the current day as available. The remaining
    keys are the preceding days, giving a re-capture window that covers Whoop's
    retroactive rescores/deletes.

    Args:
        partitions_def: A daily ``TimeWindowPartitionsDefinition``.
        before: The reference time (typically the schedule's execution time).
        count: How many trailing partitions to return.
    """
    keys: Sequence[str] = partitions_def.get_partition_keys(
        current_time=before + timedelta(days=1)
    )
    return list(keys[-count:])


def run_async[T](coro: Awaitable[T]) -> T:
    """Run an awaitable to completion from sync Dagster code.

    Each Dagster run executes in its own process with no running event loop, so a
    plain ``asyncio.run`` is correct and simplest.
    """
    return asyncio.run(coro)
