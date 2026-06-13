"""Silver weather asset (NOAA USCRN hourly, single source).

``silver_weather`` reads the raw USCRN hourly rows from the filesystem and writes one
typed, deduped Parquet — one row per hourly observation (per UTC instant). Lineage on
the bronze upstream (the ``uscrn_bronze_hourly`` asset in the *soil* code location) is
declared by ``AssetKey``; the read itself is a filesystem read of ``BRONZE_ROOT`` via
DuckDB.

Whole-table rebuild, no concurrency pool — same conventions as the other silver assets.
The DuckDB connection loads ICU so the local-day derivation (``AT TIME ZONE``) is
DST-aware.
"""

import os

from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset

from grecohome_core.silver import connect, list_payload_files, write_parquet_atomic
from grecohome_silver.config import settings
from grecohome_silver.weather import weather_sql

GROUP = "silver_weather"
WEATHER_SUBDIR = "weather"
WEATHER_PARQUET = "silver_weather.parquet"


def weather_path(filename: str) -> str:
    """Absolute path to a weather silver Parquet under ``SILVER_ROOT``."""
    return os.path.join(settings.silver_root, WEATHER_SUBDIR, filename)


@asset(name="silver_weather", group_name=GROUP, deps=[AssetKey("uscrn_bronze_hourly")])
def silver_weather(context: AssetExecutionContext) -> MaterializeResult:
    """Typed, deduped USCRN hourly weather — one row per observation (UTC instant)."""
    con = connect()
    con.execute("LOAD icu")  # DST-aware AT TIME ZONE for the derived local day
    files = list_payload_files(settings.bronze_root, "uscrn", "hourly")
    sql = weather_sql(files, timezone=settings.uscrn_timezone)
    dest = weather_path(WEATHER_PARQUET)
    rows = write_parquet_atomic(con, sql, dest, protected_root=settings.bronze_root)
    context.log.info(f"silver_weather: {rows} obs from {len(files)} bronze files -> {dest}")
    return MaterializeResult(metadata={"rows": rows, "bronze_files": len(files), "path": dest})


WEATHER_ASSETS = [silver_weather]
