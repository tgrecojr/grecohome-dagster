"""Silver glucose asset (Lingo CGM, single source).

``silver_glucose`` reads the Lingo glucose CSV exports from the filesystem and writes
one typed, deduped Parquet — one row per reading (per UTC instant). Lineage on the
bronze upstream (in the *lingo* code location) is declared by ``AssetKey``; the read
itself is a filesystem read of ``BRONZE_ROOT`` via DuckDB.

Whole-table rebuild, no concurrency pool — same conventions as the sleep assets.
"""

import os

from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset

from grecohome_core.silver import connect, list_payload_files, write_parquet_atomic
from grecohome_silver.config import settings
from grecohome_silver.glucose import glucose_sql

GROUP = "silver_glucose"
GLUCOSE_SUBDIR = "glucose"
GLUCOSE_PARQUET = "silver_glucose.parquet"


def glucose_path(filename: str) -> str:
    """Absolute path to a glucose silver Parquet under ``SILVER_ROOT``."""
    return os.path.join(settings.silver_root, GLUCOSE_SUBDIR, filename)


@asset(name="silver_glucose", group_name=GROUP, deps=[AssetKey("lingo_bronze_glucose")])
def silver_glucose(context: AssetExecutionContext) -> MaterializeResult:
    """Typed, deduped Lingo CGM — one row per reading (deduped on the UTC instant)."""
    con = connect()
    files = list_payload_files(settings.bronze_root, "lingo", "glucose")
    sql = glucose_sql(files)
    dest = glucose_path(GLUCOSE_PARQUET)
    rows = write_parquet_atomic(con, sql, dest, bronze_root=settings.bronze_root)
    context.log.info(f"silver_glucose: {rows} readings from {len(files)} bronze files -> {dest}")
    return MaterializeResult(metadata={"rows": rows, "bronze_files": len(files), "path": dest})


GLUCOSE_ASSETS = [silver_glucose]
