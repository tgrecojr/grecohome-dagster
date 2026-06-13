"""Silver asset checks for recovery — same severity convention as the other tables.

Structural/dedup correctness = ERROR; coverage = WARN. Read-only over the written
Parquet, off the ``*_api`` pools.
"""

from __future__ import annotations

import os

from dagster import AssetCheckResult, AssetCheckSeverity, asset_check

from grecohome_core.checks import alerting_check
from grecohome_core.silver import connect, list_payload_files
from grecohome_silver.config import settings
from grecohome_silver.dagster.recovery_assets import (
    RECOVERY_PARQUET,
    recovery_path,
    silver_recovery,
)
from grecohome_silver.recovery import bronze_recovery_count_sql

# Range bounds for the recovery score columns (generous physiological limits).
_RANGES = {
    "recovery_score": (0, 100),
    "resting_heart_rate": (20, 120),
    "hrv_rmssd_milli": (0, 500),
    "spo2_percentage": (50, 100),
    "skin_temp_celsius": (20, 45),
}


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


@asset_check(asset=silver_recovery, name="recovery_cycle_unique_nonnull")
@alerting_check
def recovery_cycle_unique_nonnull() -> AssetCheckResult:
    """One row per ``cycle_id``; ``cycle_id`` and ``recovery_date`` non-null."""
    path = recovery_path(RECOVERY_PARQUET)
    if not os.path.exists(path):
        return _missing(path)
    total = _scalar(f"SELECT count(*) FROM {_src(path)}")
    distinct = _scalar(f"SELECT count(DISTINCT cycle_id) FROM {_src(path)}")
    nulls = _scalar(
        f"SELECT count(*) FROM {_src(path)} WHERE cycle_id IS NULL OR recovery_date IS NULL"
    )
    return AssetCheckResult(
        passed=(total == distinct and nulls == 0),
        severity=AssetCheckSeverity.ERROR,
        metadata={"rows": total, "distinct_cycles": distinct, "null_keys": nulls},
    )


@asset_check(asset=silver_recovery, name="recovery_value_ranges")
@alerting_check
def recovery_value_ranges() -> AssetCheckResult:
    """Recovery score / RHR / HRV / SpO2 / skin temp within plausible bounds."""
    path = recovery_path(RECOVERY_PARQUET)
    if not os.path.exists(path):
        return _missing(path)
    clauses = [
        f"({c} IS NOT NULL AND ({c} < {lo} OR {c} > {hi}))" for c, (lo, hi) in _RANGES.items()
    ]
    bad = _scalar(f"SELECT count(*) FROM {_src(path)} WHERE {' OR '.join(clauses)}")
    return AssetCheckResult(
        passed=(bad == 0),
        severity=AssetCheckSeverity.ERROR,
        metadata={"out_of_range_rows": bad},
    )


@asset_check(asset=silver_recovery, name="recovery_coverage_vs_bronze")
@alerting_check
def recovery_coverage_vs_bronze() -> AssetCheckResult:
    """silver recoveries ≈ bronze distinct ``cycle_id`` — no silent drop."""
    path = recovery_path(RECOVERY_PARQUET)
    if not os.path.exists(path):
        return _missing(path)
    silver_rows = _scalar(f"SELECT count(*) FROM {_src(path)}")
    files = list_payload_files(settings.bronze_root, "whoop", "recovery")
    bronze_cycles = _scalar(bronze_recovery_count_sql(files))
    return AssetCheckResult(
        passed=(silver_rows >= bronze_cycles),
        severity=AssetCheckSeverity.WARN,
        metadata={"silver_recoveries": silver_rows, "bronze_distinct_cycles": bronze_cycles},
    )


RECOVERY_CHECKS = [
    recovery_cycle_unique_nonnull,
    recovery_value_ranges,
    recovery_coverage_vs_bronze,
]
