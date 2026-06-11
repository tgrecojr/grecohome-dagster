"""Silver sleep assets: two source-level, one unified (the copyable pattern).

``silver_sleep_garmin`` and ``silver_sleep_whoop`` read their bronze sleep streams
from the filesystem and write a typed, deduped intermediate Parquet each. The
unified ``silver_sleep`` reads those two intermediates and FULL OUTER JOINs them on
the night. Lineage is explicit:

    garmin_bronze_sleep -> silver_sleep_garmin --\\
                                                  >-- silver_sleep
    whoop_bronze_sleep  -> silver_sleep_whoop  --/

The bronze upstreams live in *other* code locations (garmin, whoop), so they are
declared by ``AssetKey`` for cross-location lineage — silver never imports those
packages; it reads ``BRONZE_ROOT`` (mounted read-only) directly via DuckDB.

Each asset is a whole-table rebuild: it overwrites its Parquet from current bronze
every run (idempotent; last run wins). No partitioning in v1 — the data is small
(thousands of nights). Assets carry **no concurrency pool**: silver makes no source
API calls and must never contend with the ``*_api`` ingestion pools.
"""

import os

from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset

from grecohome_core.silver import (
    connect,
    list_payload_files,
    payloads_relation_sql,
    write_parquet_atomic,
)
from grecohome_silver.config import settings
from grecohome_silver.sleep import garmin_sleep_sql, unified_sleep_sql, whoop_sleep_sql

GROUP = "silver_sleep"

# Layout under SILVER_ROOT (documented in docs/SILVER.md). The unified table is the
# product; the two source intermediates are named with a leading underscore.
SLEEP_SUBDIR = "sleep"
GARMIN_PARQUET = "_garmin.parquet"
WHOOP_PARQUET = "_whoop.parquet"
UNIFIED_PARQUET = "silver_sleep.parquet"


def silver_path(filename: str) -> str:
    """Absolute path to a sleep silver Parquet under ``SILVER_ROOT``."""
    return os.path.join(settings.silver_root, SLEEP_SUBDIR, filename)


def _read_parquet_sql(path: str) -> str:
    """A SELECT over a silver intermediate Parquet (single-quote escaped)."""
    return f"SELECT * FROM read_parquet('{path.replace(chr(39), chr(39) * 2)}')"


@asset(name="silver_sleep_garmin", group_name=GROUP, deps=[AssetKey("garmin_bronze_sleep")])
def silver_sleep_garmin(context: AssetExecutionContext) -> MaterializeResult:
    """Typed, deduped Garmin sleep — one row per night (~4 years of history)."""
    con = connect()
    files = list_payload_files(settings.bronze_root, "garmin", "sleep")
    sql = garmin_sleep_sql(payloads_relation_sql(files))
    dest = silver_path(GARMIN_PARQUET)
    rows = write_parquet_atomic(con, sql, dest, bronze_root=settings.bronze_root)
    context.log.info(f"silver_sleep_garmin: {rows} nights from {len(files)} bronze files -> {dest}")
    return MaterializeResult(metadata={"rows": rows, "bronze_files": len(files), "path": dest})


@asset(name="silver_sleep_whoop", group_name=GROUP, deps=[AssetKey("whoop_bronze_sleep")])
def silver_sleep_whoop(context: AssetExecutionContext) -> MaterializeResult:
    """Typed, deduped Whoop sleep — one row per sleep id, naps flagged (``is_nap``)."""
    con = connect()
    files = list_payload_files(settings.bronze_root, "whoop", "sleep")
    sql = whoop_sleep_sql(payloads_relation_sql(files))
    dest = silver_path(WHOOP_PARQUET)
    rows = write_parquet_atomic(con, sql, dest, bronze_root=settings.bronze_root)
    context.log.info(f"silver_sleep_whoop: {rows} records from {len(files)} bronze files -> {dest}")
    return MaterializeResult(metadata={"rows": rows, "bronze_files": len(files), "path": dest})


@asset(
    name="silver_sleep",
    group_name=GROUP,
    deps=[AssetKey("silver_sleep_garmin"), AssetKey("silver_sleep_whoop")],
)
def silver_sleep(context: AssetExecutionContext) -> MaterializeResult:
    """Unified nightly sleep — FULL OUTER JOIN of both sources, side by side."""
    con = connect()
    garmin_sql = _read_parquet_sql(silver_path(GARMIN_PARQUET))
    whoop_sql = _read_parquet_sql(silver_path(WHOOP_PARQUET))
    sql = unified_sleep_sql(garmin_sql, whoop_sql)
    dest = silver_path(UNIFIED_PARQUET)
    rows = write_parquet_atomic(con, sql, dest, bronze_root=settings.bronze_root)
    context.log.info(f"silver_sleep: {rows} unified nights -> {dest}")
    return MaterializeResult(metadata={"rows": rows, "path": dest})


ALL_ASSETS = [silver_sleep_garmin, silver_sleep_whoop, silver_sleep]
