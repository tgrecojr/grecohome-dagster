"""Gold daily weather mart.

``gold_daily_weather`` reads ``silver_weather`` from the filesystem and writes one
Parquet — one row per local day, with imperial + derived gardening metrics. Its silver
upstream lives in the *silver* code location, so it is declared by ``AssetKey`` for
cross-location lineage; the read is a filesystem read of ``SILVER_ROOT`` via DuckDB.
Whole-table rebuild, no pool.
"""

import os

from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset

from grecohome_core.silver import connect, write_parquet_atomic
from grecohome_gold.config import settings
from grecohome_gold.daily_weather import GDD_BASE_F, daily_weather_sql

GROUP = "gold_weather"
WEATHER_SUBDIR = "weather"
WEATHER_PARQUET = "daily_weather.parquet"


def gold_weather_path(filename: str) -> str:
    """Absolute path to a gold weather mart Parquet under ``GOLD_ROOT``."""
    return os.path.join(settings.gold_root, WEATHER_SUBDIR, filename)


@asset(name="gold_daily_weather", group_name=GROUP, deps=[AssetKey("silver_weather")])
def gold_daily_weather(context: AssetExecutionContext) -> MaterializeResult:
    """One row per local day: temp/GDD/frost, precip, solar, soil, humidity (imperial)."""
    con = connect()
    sql = daily_weather_sql(settings.silver_root)
    dest = gold_weather_path(WEATHER_PARQUET)
    rows = write_parquet_atomic(con, sql, dest, protected_root=settings.silver_root)
    context.log.info(f"gold_daily_weather: {rows} days -> {dest} (GDD base {GDD_BASE_F}F)")
    return MaterializeResult(metadata={"rows": rows, "gdd_base_f": GDD_BASE_F, "path": dest})


WEATHER_ASSETS = [gold_daily_weather]
