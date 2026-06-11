"""Silver workouts asset (Garmin activities, single source).

``silver_workouts`` reads the Garmin activities bronze and writes one typed, deduped
Parquet — one row per ``activityId``. Lineage on the bronze upstream (in the *garmin*
code location) is declared by ``AssetKey``; the read is a filesystem read of
``BRONZE_ROOT`` via DuckDB. Whole-table rebuild, no concurrency pool.
"""

import os

from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset

from grecohome_core.silver import connect, list_payload_files, write_parquet_atomic
from grecohome_silver.config import settings
from grecohome_silver.workouts import workouts_sql

GROUP = "silver_workouts"
WORKOUTS_SUBDIR = "workouts"
WORKOUTS_PARQUET = "silver_workouts.parquet"


def workouts_path(filename: str) -> str:
    """Absolute path to a workouts silver Parquet under ``SILVER_ROOT``."""
    return os.path.join(settings.silver_root, WORKOUTS_SUBDIR, filename)


@asset(name="silver_workouts", group_name=GROUP, deps=[AssetKey("garmin_bronze_activities")])
def silver_workouts(context: AssetExecutionContext) -> MaterializeResult:
    """Typed, deduped Garmin activities — one row per activity."""
    con = connect()
    files = list_payload_files(settings.bronze_root, "garmin", "activities")
    sql = workouts_sql(files)
    dest = workouts_path(WORKOUTS_PARQUET)
    rows = write_parquet_atomic(con, sql, dest, bronze_root=settings.bronze_root)
    context.log.info(f"silver_workouts: {rows} activities from {len(files)} bronze files -> {dest}")
    return MaterializeResult(metadata={"rows": rows, "bronze_files": len(files), "path": dest})


WORKOUT_ASSETS = [silver_workouts]
