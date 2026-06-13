"""Silver strain asset (Whoop cycle, single source).

``silver_strain`` reads the Whoop cycle bronze from the filesystem and writes one typed,
deduped Parquet — one row per cycle (per ``cycle_id``). Lineage on the bronze upstream
(the ``whoop_bronze_cycle`` asset in the *whoop* code location) is declared by
``AssetKey``; the read itself is a filesystem read of ``BRONZE_ROOT`` via DuckDB.

Whole-table rebuild, no concurrency pool — same conventions as the other silver assets.
"""

import os

from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset

from grecohome_core.silver import connect, list_payload_files, write_parquet_atomic
from grecohome_silver.config import settings
from grecohome_silver.strain import strain_sql

GROUP = "silver_strain"
STRAIN_SUBDIR = "strain"
STRAIN_PARQUET = "silver_strain.parquet"


def strain_path(filename: str) -> str:
    """Absolute path to a strain silver Parquet under ``SILVER_ROOT``."""
    return os.path.join(settings.silver_root, STRAIN_SUBDIR, filename)


@asset(name="silver_strain", group_name=GROUP, deps=[AssetKey("whoop_bronze_cycle")])
def silver_strain(context: AssetExecutionContext) -> MaterializeResult:
    """Typed, deduped Whoop strain — one row per cycle (deduped on the latest rescore)."""
    con = connect()
    files = list_payload_files(settings.bronze_root, "whoop", "cycle")
    sql = strain_sql(files)
    dest = strain_path(STRAIN_PARQUET)
    rows = write_parquet_atomic(con, sql, dest, protected_root=settings.bronze_root)
    context.log.info(f"silver_strain: {rows} cycles from {len(files)} bronze files -> {dest}")
    return MaterializeResult(metadata={"rows": rows, "bronze_files": len(files), "path": dest})


STRAIN_ASSETS = [silver_strain]
