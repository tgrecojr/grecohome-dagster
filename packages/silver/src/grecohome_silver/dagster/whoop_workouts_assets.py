"""Silver Whoop-workouts asset (Whoop activities, single source).

``silver_whoop_workouts`` reads the Whoop workout bronze from the filesystem and writes one
typed, deduped Parquet — one row per Whoop workout. Lineage on the bronze upstream (the
``whoop_bronze_workout`` asset in the *whoop* code location) is declared by ``AssetKey``;
the read itself is a filesystem read of ``BRONZE_ROOT`` via DuckDB.

Kept separate from ``silver_workouts`` (Garmin) on purpose — two devices, neither
authoritative, never blended (the sleep philosophy). Whole-table rebuild, no pool.
"""

import os

from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset

from grecohome_core.silver import connect, list_payload_files, write_parquet_atomic
from grecohome_silver.config import settings
from grecohome_silver.whoop_workouts import whoop_workouts_sql

GROUP = "silver_whoop_workouts"
WHOOP_WORKOUTS_SUBDIR = "whoop_workouts"
WHOOP_WORKOUTS_PARQUET = "silver_whoop_workouts.parquet"


def whoop_workouts_path(filename: str) -> str:
    """Absolute path to a Whoop-workouts silver Parquet under ``SILVER_ROOT``."""
    return os.path.join(settings.silver_root, WHOOP_WORKOUTS_SUBDIR, filename)


@asset(
    name="silver_whoop_workouts",
    group_name=GROUP,
    deps=[AssetKey("whoop_bronze_workout")],
)
def silver_whoop_workouts(context: AssetExecutionContext) -> MaterializeResult:
    """Typed, deduped Whoop workouts — one row per workout (latest rescore)."""
    con = connect()
    files = list_payload_files(settings.bronze_root, "whoop", "workout")
    sql = whoop_workouts_sql(files)
    dest = whoop_workouts_path(WHOOP_WORKOUTS_PARQUET)
    rows = write_parquet_atomic(con, sql, dest, protected_root=settings.bronze_root)
    context.log.info(
        f"silver_whoop_workouts: {rows} workouts from {len(files)} bronze files -> {dest}"
    )
    return MaterializeResult(metadata={"rows": rows, "bronze_files": len(files), "path": dest})


WHOOP_WORKOUTS_ASSETS = [silver_whoop_workouts]
