"""Silver location asset (Overland + OwnTracks points, reverse-geocode enriched).

``silver_location`` reads the two ``location`` bronze point streams and the ``geocode``
bronze cache from the filesystem and writes one typed Parquet — one row per fix, each
LEFT JOINed to its cell's nearest Photon place. Cross-location lineage on the three bronze
upstreams is declared by ``AssetKey``; the reads are filesystem reads of ``BRONZE_ROOT``
via DuckDB (no gRPC, no network — the Photon calls already happened in geocode bronze).

Whole-table rebuild, no concurrency pool — same conventions as the other silver assets.
"""

import glob
import os

from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset

from grecohome_core.silver import connect, list_payload_files, write_parquet_atomic
from grecohome_silver.config import settings
from grecohome_silver.location import location_sql

GROUP = "silver_location"
LOCATION_SUBDIR = "location"
LOCATION_PARQUET = "silver_location.parquet"


def location_path(filename: str) -> str:
    """Absolute path to a location silver Parquet under ``SILVER_ROOT``."""
    return os.path.join(settings.silver_root, LOCATION_SUBDIR, filename)


def list_sidecar_files(bronze_root: str, source: str, collection: str) -> list[str]:
    """Every ``.meta.json`` sidecar for a collection across all ``dt=`` partitions, sorted.

    The geocode cache's cell key (``lat_e4``/``lon_e4``) lives in the sidecar, so silver
    reads the sidecars here (the complement of ``list_payload_files``, which excludes them).
    """
    pattern = os.path.join(bronze_root, source, collection, "dt=*", "*.meta.json")
    return sorted(f for f in glob.glob(pattern) if os.path.isfile(f))


@asset(
    name="silver_location",
    group_name=GROUP,
    deps=[
        AssetKey("location_bronze_overland"),
        AssetKey("location_bronze_owntracks"),
        AssetKey("geocode_bronze_reverse"),
    ],
)
def silver_location(context: AssetExecutionContext) -> MaterializeResult:
    """Typed location fixes enriched with reverse-geocoded place (one row per fix)."""
    con = connect()
    overland = list_payload_files(settings.bronze_root, "location", "overland")
    owntracks = list_payload_files(settings.bronze_root, "location", "owntracks")
    geo_payloads = list_payload_files(settings.bronze_root, "geocode", "reverse")
    geo_sidecars = list_sidecar_files(settings.bronze_root, "geocode", "reverse")

    sql = location_sql(overland, owntracks, geo_payloads, geo_sidecars)
    dest = location_path(LOCATION_PARQUET)
    rows = write_parquet_atomic(con, sql, dest, protected_root=settings.bronze_root)
    context.log.info(
        f"silver_location: {rows} fixes from {len(overland) + len(owntracks)} bronze "
        f"point files, {len(geo_payloads)} cached cells -> {dest}"
    )
    return MaterializeResult(
        metadata={
            "rows": rows,
            "overland_files": len(overland),
            "owntracks_files": len(owntracks),
            "geocode_cells": len(geo_payloads),
            "path": dest,
        }
    )


LOCATION_ASSETS = [silver_location]
