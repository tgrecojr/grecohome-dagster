"""Bronze assets for the Whoop data subject.

Each range collection (sleep/recovery/workout/cycle) is a daily UTC-partitioned
asset that fetches its partition's fetch-window from the Whoop API; capture to the
bronze layer happens inside the client. Profile and body measurement are
"current-only" snapshots, modeled as one unpartitioned asset.

All API-touching assets share the ``whoop_api`` concurrency pool so the hourly tick
and any backfill can't collectively exceed the API budget (and only one run holds
the OAuth token at a time). The pool's limit is enforced by the host Dagster
instance, not here -- see docs/DEPLOYMENT.md.
"""

from dagster import AssetExecutionContext, AssetsDefinition, asset

from grecohome_core.dagster.helpers import daily_utc_partitions, run_async
from grecohome_whoop.api.whoop_client import WhoopClient

# Backfill floor; partitions are UTC fetch-slices, not local days. end_offset=1
# makes the in-progress current day a valid, materializable partition so the
# hourly schedule can re-capture intraday data.
WHOOP_PARTITIONS_START = "2024-01-01"
WHOOP_DAILY = daily_utc_partitions(WHOOP_PARTITIONS_START, end_offset=1)

# Shared Whoop-API concurrency pool (limit enforced on the host instance).
WHOOP_POOL = "whoop_api"
BRONZE_GROUP = "whoop_bronze"

# (collection, WhoopClient method) for the daily-partitioned range collections.
RANGE_COLLECTIONS: list[tuple[str, str]] = [
    ("sleep", "get_sleep_records"),
    ("recovery", "get_recovery_records"),
    ("workout", "get_workout_records"),
    ("cycle", "get_cycle_records"),
]


async def _fetch_range(collection: str, method: str, start, end, dt: str) -> int:
    """Fetch one collection's partition window; capture happens inside the client."""
    client = WhoopClient(user_id=1, bronze_dt=dt)
    try:
        records = await getattr(client, method)(start=start, end=end)
        return len(records)
    finally:
        await client.aclose()


def _make_bronze_asset(collection: str, method: str) -> AssetsDefinition:
    """Build a daily-partitioned bronze asset for one range collection."""

    @asset(
        name=f"whoop_bronze_{collection}",
        partitions_def=WHOOP_DAILY,
        pool=WHOOP_POOL,
        group_name=BRONZE_GROUP,
    )
    def _bronze_asset(context: AssetExecutionContext) -> None:
        tw = context.partition_time_window
        dt = context.partition_key
        count = run_async(_fetch_range(collection, method, tw.start, tw.end, dt))
        context.add_output_metadata({"records": count, "partition": dt})

    return _bronze_asset


bronze_sleep = _make_bronze_asset("sleep", "get_sleep_records")
bronze_recovery = _make_bronze_asset("recovery", "get_recovery_records")
bronze_workout = _make_bronze_asset("workout", "get_workout_records")
bronze_cycle = _make_bronze_asset("cycle", "get_cycle_records")

RANGE_ASSETS: list[AssetsDefinition] = [
    bronze_sleep,
    bronze_recovery,
    bronze_workout,
    bronze_cycle,
]


async def _fetch_snapshots() -> dict[str, bool]:
    """Capture the current profile + body measurement (no date partition)."""
    client = WhoopClient(user_id=1)  # bronze_dt=None -> fetch-time date folder
    try:
        await client.get_user_profile()
        await client.get_body_measurement()
        return {"profile": True, "body_measurement": True}
    finally:
        await client.aclose()


@asset(name="whoop_bronze_snapshots", pool=WHOOP_POOL, group_name=BRONZE_GROUP)
def bronze_snapshots(context: AssetExecutionContext) -> None:
    """Capture current-only Whoop snapshots (profile, body measurement)."""
    captured = run_async(_fetch_snapshots())
    context.add_output_metadata(captured)
