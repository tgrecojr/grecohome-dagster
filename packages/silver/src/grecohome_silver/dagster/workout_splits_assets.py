"""Silver workout-splits asset (Garmin activity splits, single source).

``silver_workout_splits`` reads the Garmin activity-splits bronze from the filesystem and
writes one typed, deduped Parquet — one row per lap. Lineage on the bronze upstream (the
``garmin_bronze_activity_splits`` asset in the *garmin* code location) is declared by
``AssetKey``; the read itself is a filesystem read of ``BRONZE_ROOT`` via DuckDB.

Whole-table rebuild, no concurrency pool — same conventions as the other silver assets.
"""

import os

from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset

from grecohome_core.silver import connect, list_payload_files, write_parquet_atomic
from grecohome_silver.config import settings
from grecohome_silver.workout_splits import splits_sql

GROUP = "silver_workout_splits"
SPLITS_SUBDIR = "workout_splits"
SPLITS_PARQUET = "silver_workout_splits.parquet"


def splits_path(filename: str) -> str:
    """Absolute path to a workout-splits silver Parquet under ``SILVER_ROOT``."""
    return os.path.join(settings.silver_root, SPLITS_SUBDIR, filename)


@asset(
    name="silver_workout_splits",
    group_name=GROUP,
    deps=[AssetKey("garmin_bronze_activity_splits")],
)
def silver_workout_splits(context: AssetExecutionContext) -> MaterializeResult:
    """Typed, deduped Garmin laps — one row per (activity_id, lap_index)."""
    con = connect()
    files = list_payload_files(settings.bronze_root, "garmin", "activity_splits")
    sql = splits_sql(files)
    dest = splits_path(SPLITS_PARQUET)
    rows = write_parquet_atomic(con, sql, dest, protected_root=settings.bronze_root)
    context.log.info(f"silver_workout_splits: {rows} laps from {len(files)} bronze files -> {dest}")
    return MaterializeResult(metadata={"rows": rows, "bronze_files": len(files), "path": dest})


WORKOUT_SPLITS_ASSETS = [silver_workout_splits]
