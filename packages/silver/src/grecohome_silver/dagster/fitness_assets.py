"""Silver fitness-snapshot asset (Garmin, multi-collection).

``silver_fitness`` reads three Garmin snapshot collections (max_metrics, training_status,
race_predictions) from the filesystem and writes one typed Parquet — one row per snapshot
day. Lineage on the three bronze upstreams (in the *garmin* code location) is declared by
``AssetKey``; the reads are filesystem reads of ``BRONZE_ROOT`` via DuckDB.

Whole-table rebuild, no concurrency pool — same conventions as the other silver assets.
"""

import os

from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset

from grecohome_core.silver import connect, list_payload_files, write_parquet_atomic
from grecohome_silver.config import settings
from grecohome_silver.fitness import fitness_sql

GROUP = "silver_fitness"
FITNESS_SUBDIR = "fitness"
FITNESS_PARQUET = "silver_fitness.parquet"

_DEPS = [
    AssetKey("garmin_bronze_max_metrics"),
    AssetKey("garmin_bronze_training_status"),
    AssetKey("garmin_bronze_race_predictions"),
]


def fitness_path(filename: str) -> str:
    """Absolute path to a fitness silver Parquet under ``SILVER_ROOT``."""
    return os.path.join(settings.silver_root, FITNESS_SUBDIR, filename)


@asset(name="silver_fitness", group_name=GROUP, deps=_DEPS)
def silver_fitness(context: AssetExecutionContext) -> MaterializeResult:
    """Typed Garmin fitness snapshots — one row per snapshot day (VO2max / status / race)."""
    con = connect()
    mm = list_payload_files(settings.bronze_root, "garmin", "max_metrics")
    ts = list_payload_files(settings.bronze_root, "garmin", "training_status")
    rp = list_payload_files(settings.bronze_root, "garmin", "race_predictions")
    sql = fitness_sql(mm, ts, rp)
    dest = fitness_path(FITNESS_PARQUET)
    rows = write_parquet_atomic(con, sql, dest, protected_root=settings.bronze_root)
    context.log.info(
        f"silver_fitness: {rows} snapshot days from {len(mm) + len(ts) + len(rp)} bronze files "
        f"-> {dest}"
    )
    return MaterializeResult(metadata={"rows": rows, "path": dest})


FITNESS_ASSETS = [silver_fitness]
