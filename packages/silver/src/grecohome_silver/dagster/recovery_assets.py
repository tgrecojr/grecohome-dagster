"""Silver recovery asset (Whoop recovery, single source).

``silver_recovery`` reads the Whoop recovery bronze and writes one typed, deduped
Parquet — one row per ``cycle_id``. Lineage on the bronze upstream (in the *whoop*
code location) is declared by ``AssetKey``; the read is a filesystem read of
``BRONZE_ROOT`` via DuckDB. Whole-table rebuild, no concurrency pool.
"""

import os

from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset

from grecohome_core.silver import connect, list_payload_files, write_parquet_atomic
from grecohome_silver.config import settings
from grecohome_silver.recovery import recovery_sql

GROUP = "silver_recovery"
RECOVERY_SUBDIR = "recovery"
RECOVERY_PARQUET = "silver_recovery.parquet"


def recovery_path(filename: str) -> str:
    """Absolute path to a recovery silver Parquet under ``SILVER_ROOT``."""
    return os.path.join(settings.silver_root, RECOVERY_SUBDIR, filename)


@asset(name="silver_recovery", group_name=GROUP, deps=[AssetKey("whoop_bronze_recovery")])
def silver_recovery(context: AssetExecutionContext) -> MaterializeResult:
    """Typed, deduped Whoop recovery — one row per cycle."""
    con = connect()
    files = list_payload_files(settings.bronze_root, "whoop", "recovery")
    sql = recovery_sql(files)
    dest = recovery_path(RECOVERY_PARQUET)
    rows = write_parquet_atomic(con, sql, dest, bronze_root=settings.bronze_root)
    context.log.info(f"silver_recovery: {rows} cycles from {len(files)} bronze files -> {dest}")
    return MaterializeResult(metadata={"rows": rows, "bronze_files": len(files), "path": dest})


RECOVERY_ASSETS = [silver_recovery]
