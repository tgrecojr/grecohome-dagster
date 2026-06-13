"""Silver body asset (Garmin weigh-ins, single source).

``silver_body`` reads the Garmin weigh-in bronze from the filesystem and writes one typed,
deduped Parquet — one row per weigh-in (per ``sample_pk``). Lineage on the bronze upstream
(the ``garmin_bronze_daily_weigh_ins`` asset in the *garmin* code location) is declared by
``AssetKey``; the read itself is a filesystem read of ``BRONZE_ROOT`` via DuckDB.

Whole-table rebuild, no concurrency pool — same conventions as the other silver assets.
"""

import os

from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset

from grecohome_core.silver import connect, list_payload_files, write_parquet_atomic
from grecohome_silver.body import body_sql
from grecohome_silver.config import settings

GROUP = "silver_body"
BODY_SUBDIR = "body"
BODY_PARQUET = "silver_body.parquet"


def body_path(filename: str) -> str:
    """Absolute path to a body silver Parquet under ``SILVER_ROOT``."""
    return os.path.join(settings.silver_root, BODY_SUBDIR, filename)


@asset(name="silver_body", group_name=GROUP, deps=[AssetKey("garmin_bronze_daily_weigh_ins")])
def silver_body(context: AssetExecutionContext) -> MaterializeResult:
    """Typed, deduped Garmin weigh-ins — one row per weigh-in (latest fetch)."""
    con = connect()
    files = list_payload_files(settings.bronze_root, "garmin", "daily_weigh_ins")
    sql = body_sql(files)
    dest = body_path(BODY_PARQUET)
    rows = write_parquet_atomic(con, sql, dest, protected_root=settings.bronze_root)
    context.log.info(f"silver_body: {rows} weigh-ins from {len(files)} bronze files -> {dest}")
    return MaterializeResult(metadata={"rows": rows, "bronze_files": len(files), "path": dest})


BODY_ASSETS = [silver_body]
