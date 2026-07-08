"""Bronze asset for the Photon reverse-geocode cache.

One unpartitioned, scheduled asset. Each run discovers cells observed in the ``location``
bronze streams that aren't cached yet and looks each up on Photon, caching the raw
response to bronze. Idempotency is the cache itself (a cell recorded in a sidecar is
never re-queried), so the asset carries no Dagster partitions. It declares cross-location
lineage on the two ``location`` bronze assets by ``AssetKey`` — the reads are filesystem
reads of ``BRONZE_ROOT/location``, not gRPC calls (same pattern silver uses on bronze).

A single-slot pool keeps two overlapping runs from double-looking-up the same cell.
"""

from dagster import AssetExecutionContext, AssetKey, asset

from grecohome_geocode.config import settings
from grecohome_geocode.geocode import GeocodeReport, geocode_cells

#: Single-slot pool (limit enforced host-side) so overlapping runs don't double-look-up.
GEOCODE_POOL = "geocode"


def _report_metadata(report: GeocodeReport) -> dict:
    return {
        "new_cells": report.new_cells,
        "looked_up": report.looked_up,
        "captured": report.captured,
        "failed": report.failed,
        "capped": report.capped,
    }


@asset(
    pool=GEOCODE_POOL,
    group_name="geocode",
    deps=[AssetKey("location_bronze_overland"), AssetKey("location_bronze_owntracks")],
)
def geocode_bronze_reverse(context: AssetExecutionContext) -> None:
    """Cache Photon reverse-geocode responses for newly-observed location cells."""
    report = geocode_cells(
        bronze_root=settings.bronze_root,
        photon_base_url=settings.photon_base_url,
        scan_days=settings.geocode_scan_days,
        max_lookups=settings.geocode_max_lookups_per_run,
        timeout=settings.photon_timeout,
        language=settings.photon_language,
        radius_km=settings.photon_radius_km,
    )
    context.add_output_metadata(_report_metadata(report))
