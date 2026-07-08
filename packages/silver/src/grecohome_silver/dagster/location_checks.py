"""Silver asset checks for location — same severity convention as the other silver checks.

Structural/dedup correctness = ERROR; coverage/expectation drift = WARN. Read-only over
the written Parquet.
"""

from __future__ import annotations

import os

from dagster import AssetCheckResult, AssetCheckSeverity, asset_check

from grecohome_core.checks import alerting_check
from grecohome_core.silver import connect, list_payload_files
from grecohome_silver.config import settings
from grecohome_silver.dagster.location_assets import (
    LOCATION_PARQUET,
    list_sidecar_files,
    location_path,
    silver_location,
)
from grecohome_silver.location import bronze_point_count_sql


def _missing(path: str) -> AssetCheckResult:
    return AssetCheckResult(
        passed=False,
        severity=AssetCheckSeverity.ERROR,
        metadata={"error": f"silver output missing: {path}"},
    )


def _scalar(sql: str) -> int:
    return int(connect().execute(sql).fetchone()[0])


def _src(path: str) -> str:
    return f"read_parquet('{path.replace(chr(39), chr(39) * 2)}')"


@asset_check(asset=silver_location, name="location_fix_unique_nonnull")
@alerting_check
def location_fix_unique_nonnull() -> AssetCheckResult:
    """One row per fix ``(source_stream, event_ts_utc, lat, lon)``; keys non-null."""
    path = location_path(LOCATION_PARQUET)
    if not os.path.exists(path):
        return _missing(path)
    total = _scalar(f"SELECT count(*) FROM {_src(path)}")
    distinct = _scalar(
        "SELECT count(DISTINCT source_stream || '|' || CAST(event_ts_utc AS VARCHAR) "
        f"|| '|' || CAST(lat AS VARCHAR) || '|' || CAST(lon AS VARCHAR)) FROM {_src(path)}"
    )
    nulls = _scalar(
        f"SELECT count(*) FROM {_src(path)} "
        "WHERE event_ts_utc IS NULL OR lat IS NULL OR lon IS NULL"
    )
    return AssetCheckResult(
        passed=(total == distinct and nulls == 0),
        severity=AssetCheckSeverity.ERROR,
        metadata={"rows": total, "distinct_fixes": distinct, "null_keys": nulls},
    )


@asset_check(asset=silver_location, name="location_coord_range")
@alerting_check
def location_coord_range() -> AssetCheckResult:
    """Latitudes in [-90, 90] and longitudes in [-180, 180]."""
    path = location_path(LOCATION_PARQUET)
    if not os.path.exists(path):
        return _missing(path)
    bad = _scalar(
        f"SELECT count(*) FROM {_src(path)} "
        "WHERE lat < -90 OR lat > 90 OR lon < -180 OR lon > 180"
    )
    return AssetCheckResult(
        passed=(bad == 0),
        severity=AssetCheckSeverity.ERROR,
        metadata={"out_of_range_rows": bad},
    )


@asset_check(asset=silver_location, name="location_coverage_vs_bronze")
@alerting_check
def location_coverage_vs_bronze() -> AssetCheckResult:
    """silver fixes ≈ bronze distinct fixes (no silent drop) + geocode coverage (WARN)."""
    path = location_path(LOCATION_PARQUET)
    if not os.path.exists(path):
        return _missing(path)
    silver_rows = _scalar(f"SELECT count(*) FROM {_src(path)}")
    overland = list_payload_files(settings.bronze_root, "location", "overland")
    owntracks = list_payload_files(settings.bronze_root, "location", "owntracks")
    bronze_fixes = _scalar(bronze_point_count_sql(overland, owntracks))
    geocoded = _scalar(f"SELECT count(*) FROM {_src(path)} WHERE geocoded")
    resolved = _scalar(f"SELECT count(*) FROM {_src(path)} WHERE geo_name IS NOT NULL")
    cells = len(list_sidecar_files(settings.bronze_root, "geocode", "reverse"))
    return AssetCheckResult(
        passed=(silver_rows >= bronze_fixes),
        severity=AssetCheckSeverity.WARN,
        metadata={
            "silver_fixes": silver_rows,
            "bronze_distinct_fixes": bronze_fixes,
            "geocoded_fixes": geocoded,
            "named_fixes": resolved,
            "cached_cells": cells,
        },
    )


LOCATION_CHECKS = [
    location_fix_unique_nonnull,
    location_coord_range,
    location_coverage_vs_bronze,
]
