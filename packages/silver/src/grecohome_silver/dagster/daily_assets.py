"""Silver daily-summary asset (Garmin user_summary, single source).

``silver_daily`` reads the Garmin daily-summary bronze from the filesystem and writes one
typed, deduped Parquet — one row per local day. Lineage on the bronze upstream (the
``garmin_bronze_user_summary`` asset in the *garmin* code location) is declared by
``AssetKey``; the read itself is a filesystem read of ``BRONZE_ROOT`` via DuckDB.

Whole-table rebuild, no concurrency pool — same conventions as the other silver assets.
"""

import os

from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset

from grecohome_core.silver import connect, list_payload_files, write_parquet_atomic
from grecohome_silver.config import settings
from grecohome_silver.daily import daily_sql

GROUP = "silver_daily"
DAILY_SUBDIR = "daily"
DAILY_PARQUET = "silver_daily.parquet"


def daily_path(filename: str) -> str:
    """Absolute path to a daily-summary silver Parquet under ``SILVER_ROOT``."""
    return os.path.join(settings.silver_root, DAILY_SUBDIR, filename)


@asset(name="silver_daily", group_name=GROUP, deps=[AssetKey("garmin_bronze_user_summary")])
def silver_daily(context: AssetExecutionContext) -> MaterializeResult:
    """Typed, deduped Garmin daily summary — one row per local day (latest fetch)."""
    con = connect()
    files = list_payload_files(settings.bronze_root, "garmin", "user_summary")
    sql = daily_sql(files)
    dest = daily_path(DAILY_PARQUET)
    rows = write_parquet_atomic(con, sql, dest, protected_root=settings.bronze_root)
    context.log.info(f"silver_daily: {rows} days from {len(files)} bronze files -> {dest}")
    return MaterializeResult(metadata={"rows": rows, "bronze_files": len(files), "path": dest})


DAILY_ASSETS = [silver_daily]
