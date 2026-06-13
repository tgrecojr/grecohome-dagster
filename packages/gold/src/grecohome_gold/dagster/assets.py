"""Gold assets. v1: the daily wellness mart.

``gold_daily_wellness`` reads the seven silver tables from the filesystem and writes one
Parquet — one row per local day. Its silver upstreams live in the *silver* code
location, so they are declared by ``AssetKey`` for cross-location lineage; the reads
are filesystem reads of ``SILVER_ROOT`` via DuckDB. Whole-table rebuild, no pool.
"""

import os

from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset

from grecohome_core.silver import connect, write_parquet_atomic
from grecohome_gold.config import settings
from grecohome_gold.daily_wellness import TIR_HIGH, TIR_LOW, daily_wellness_sql

GROUP = "gold_wellness"
WELLNESS_SUBDIR = "wellness"
WELLNESS_PARQUET = "daily_wellness.parquet"

_SILVER_DEPS = [
    AssetKey("silver_sleep"),
    AssetKey("silver_recovery"),
    AssetKey("silver_workouts"),
    AssetKey("silver_glucose"),
    AssetKey("silver_strain"),
    AssetKey("silver_daily"),
    AssetKey("silver_body"),
]


def gold_path(filename: str) -> str:
    """Absolute path to a gold mart Parquet under ``GOLD_ROOT``."""
    return os.path.join(settings.gold_root, WELLNESS_SUBDIR, filename)


@asset(name="gold_daily_wellness", group_name=GROUP, deps=_SILVER_DEPS)
def gold_daily_wellness(context: AssetExecutionContext) -> MaterializeResult:
    """One row/day: sleep + recovery + strain + daily activity + workouts + glucose + weight."""
    con = connect()
    sql = daily_wellness_sql(settings.silver_root)
    dest = gold_path(WELLNESS_PARQUET)
    rows = write_parquet_atomic(con, sql, dest, protected_root=settings.silver_root)
    context.log.info(f"gold_daily_wellness: {rows} days -> {dest} (TIR {TIR_LOW}-{TIR_HIGH})")
    return MaterializeResult(
        metadata={"rows": rows, "tir_range": f"{TIR_LOW}-{TIR_HIGH}", "path": dest}
    )


ALL_ASSETS = [gold_daily_wellness]
