"""Per-collection bronze assets, generated from the catalog.

Every catalog endpoint becomes its own bronze asset (``garmin_bronze_<collection>``):
date-oriented endpoints (daily / range / per-device-range / activities) are
daily-UTC-partitioned; reference endpoints (static / goals / weekly / per-profile /
per-device) are unpartitioned. ``activities`` fans out per-activity detail + the FIT
download internally. Each asset uses the shared per-run Garmin client resource and
captures append-only (no dedup -- Garmin data is immutable).

``FETCH_SELECTION`` / ``FETCH_EXCLUDE`` are honored at runtime: a deselected asset
materializes as a no-op (no capture), keeping the asset graph stable.
"""

from datetime import UTC, datetime

from dagster import AssetExecutionContext, AssetsDefinition, asset

from grecohome_core.dagster.helpers import daily_utc_partitions
from grecohome_garmin import catalog
from grecohome_garmin.catalog import (
    KIND_DAILY,
    KIND_PER_DEVICE,
    KIND_PER_DEVICE_RANGE,
    KIND_PER_PROFILE,
    KIND_RANGE,
    KIND_STATIC,
    KIND_STATIC_GOALS,
    KIND_WEEKLY,
    Endpoint,
)
from grecohome_garmin.config import settings
from grecohome_garmin.pull import GarminPuller

# Backfill floor; daily UTC fetch-slices, end_offset=0 so only completed days are
# partitions (we never freeze a partial "today").
GARMIN_PARTITIONS_START = "2024-01-01"
GARMIN_DAILY = daily_utc_partitions(GARMIN_PARTITIONS_START)

# Shared Garmin-API concurrency pool (limit enforced on the host instance).
GARMIN_POOL = "garmin_api"

# Kinds whose data is date-oriented -> daily-partitioned assets.
_DAILY_KINDS = frozenset({KIND_DAILY, KIND_RANGE, KIND_PER_DEVICE_RANGE})


def _puller(context: AssetExecutionContext) -> GarminPuller:
    return GarminPuller(context.resources.garmin, settings)


def _run_partitioned(context: AssetExecutionContext, ep: Endpoint) -> None:
    d = context.partition_key
    puller = _puller(context)
    if ep.name == "activities":
        puller.pull_activities(d, d, dt=d)  # discovers the day + fans out per activity
    elif ep.kind == KIND_PER_DEVICE_RANGE:
        puller.pull_per_device(ep, start=d, end=d, dt=d)
    elif ep.kind == KIND_DAILY:
        puller.pull_endpoint(ep, cdate=d)
    elif ep.kind == KIND_RANGE:
        puller.pull_endpoint(ep, start=d, end=d, dt=d)


def _run_reference(context: AssetExecutionContext, ep: Endpoint) -> None:
    puller = _puller(context)
    if ep.kind == KIND_WEEKLY:
        end = datetime.now(UTC).date().isoformat()
        puller.pull_endpoint(ep, end=end)  # trailing weekly_weeks ending today
    elif ep.kind in (KIND_STATIC, KIND_STATIC_GOALS):
        puller.pull_endpoint(ep)
    elif ep.kind == KIND_PER_PROFILE:
        puller.pull_per_profile(ep)
    elif ep.kind == KIND_PER_DEVICE:
        puller.pull_per_device(ep)


def _make_asset(ep: Endpoint) -> AssetsDefinition:
    """Build one bronze asset for a catalog endpoint, partitioned per its kind."""
    is_daily = ep.kind in _DAILY_KINDS
    group = (
        "garmin_activities"
        if ep.name == "activities"
        else ("garmin_daily" if is_daily else "garmin_reference")
    )
    common = {
        "name": f"garmin_bronze_{ep.collection}",
        "pool": GARMIN_POOL,
        "group_name": group,
        "required_resource_keys": {"garmin"},
    }

    if is_daily:

        @asset(partitions_def=GARMIN_DAILY, **common)
        def _daily_asset(context: AssetExecutionContext) -> None:
            if not settings.is_selected(ep.name):
                context.log.info(f"garmin endpoint not selected, skipping: {ep.name}")
                return
            _run_partitioned(context, ep)

        return _daily_asset

    @asset(**common)
    def _reference_asset(context: AssetExecutionContext) -> None:
        if not settings.is_selected(ep.name):
            context.log.info(f"garmin endpoint not selected, skipping: {ep.name}")
            return
        _run_reference(context, ep)

    return _reference_asset


DAILY_ASSETS: list[AssetsDefinition] = [
    _make_asset(ep) for ep in catalog.CATALOG if ep.kind in _DAILY_KINDS
]
REFERENCE_ASSETS: list[AssetsDefinition] = [
    _make_asset(ep) for ep in catalog.CATALOG if ep.kind not in _DAILY_KINDS
]
ALL_ASSETS: list[AssetsDefinition] = DAILY_ASSETS + REFERENCE_ASSETS

# Lookup by collection (handy for tests / targeted materialization).
ASSET_BY_COLLECTION: dict[str, AssetsDefinition] = {
    ep.collection: a
    for ep, a in zip(
        [ep for ep in catalog.CATALOG if ep.kind in _DAILY_KINDS]
        + [ep for ep in catalog.CATALOG if ep.kind not in _DAILY_KINDS],
        ALL_ASSETS,
        strict=True,
    )
}
