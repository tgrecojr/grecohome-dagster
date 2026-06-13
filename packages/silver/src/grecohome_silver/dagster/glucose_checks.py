"""Silver asset checks for glucose — same severity convention as the bronze/sleep checks.

Structural/dedup correctness = ERROR; coverage/expectation drift = WARN. Read-only over
the written Parquet, off the ``*_api`` pools.
"""

from __future__ import annotations

import os

from dagster import AssetCheckResult, AssetCheckSeverity, asset_check

from grecohome_core.checks import alerting_check
from grecohome_core.silver import connect, list_payload_files
from grecohome_silver.config import settings
from grecohome_silver.dagster.glucose_assets import GLUCOSE_PARQUET, glucose_path, silver_glucose
from grecohome_silver.glucose import bronze_reading_count_sql


def _missing(path: str) -> AssetCheckResult:
    return AssetCheckResult(
        passed=False,
        severity=AssetCheckSeverity.ERROR,
        metadata={"error": f"silver output missing: {path}"},
    )


def _scalar(sql: str) -> int:
    return int(connect().execute(sql).fetchone()[0])


def _src(path: str) -> str:
    return f"read_parquet('{path.replace(chr(39), chr(39) * 2)}')"


@asset_check(asset=silver_glucose, name="glucose_reading_unique_nonnull")
@alerting_check
def glucose_reading_unique_nonnull() -> AssetCheckResult:
    """One row per reading (UTC instant); ``reading_ts_utc`` and ``reading_date`` non-null."""
    path = glucose_path(GLUCOSE_PARQUET)
    if not os.path.exists(path):
        return _missing(path)
    total = _scalar(f"SELECT count(*) FROM {_src(path)}")
    distinct = _scalar(f"SELECT count(DISTINCT reading_ts_utc) FROM {_src(path)}")
    nulls = _scalar(
        f"SELECT count(*) FROM {_src(path)} WHERE reading_ts_utc IS NULL OR reading_date IS NULL"
    )
    return AssetCheckResult(
        passed=(total == distinct and nulls == 0),
        severity=AssetCheckSeverity.ERROR,
        metadata={"rows": total, "distinct_instants": distinct, "null_keys": nulls},
    )


@asset_check(asset=silver_glucose, name="glucose_value_range")
@alerting_check
def glucose_value_range() -> AssetCheckResult:
    """Non-null ``mgdl`` within a generous physiological range (10–600)."""
    path = glucose_path(GLUCOSE_PARQUET)
    if not os.path.exists(path):
        return _missing(path)
    bad = _scalar(
        f"SELECT count(*) FROM {_src(path)} WHERE mgdl IS NOT NULL AND (mgdl < 10 OR mgdl > 600)"
    )
    return AssetCheckResult(
        passed=(bad == 0),
        severity=AssetCheckSeverity.ERROR,
        metadata={"out_of_range_rows": bad},
    )


@asset_check(asset=silver_glucose, name="glucose_coverage_vs_bronze")
@alerting_check
def glucose_coverage_vs_bronze() -> AssetCheckResult:
    """silver readings ≈ bronze distinct readings (UTC instants) — no silent drop."""
    path = glucose_path(GLUCOSE_PARQUET)
    if not os.path.exists(path):
        return _missing(path)
    silver_rows = _scalar(f"SELECT count(*) FROM {_src(path)}")
    files = list_payload_files(settings.bronze_root, "lingo", "glucose")
    bronze_readings = _scalar(bronze_reading_count_sql(files))
    return AssetCheckResult(
        passed=(silver_rows >= bronze_readings),
        severity=AssetCheckSeverity.WARN,
        metadata={
            "silver_readings": silver_rows,
            "bronze_distinct_readings": bronze_readings,
            "distinct_days": _scalar(f"SELECT count(DISTINCT reading_date) FROM {_src(path)}"),
        },
    )


GLUCOSE_CHECKS = [
    glucose_reading_unique_nonnull,
    glucose_value_range,
    glucose_coverage_vs_bronze,
]
