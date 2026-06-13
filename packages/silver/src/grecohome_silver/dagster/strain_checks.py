"""Silver asset checks for strain — same severity convention as the other silver checks.

Structural/dedup correctness = ERROR; coverage/expectation drift = WARN. Read-only over
the written Parquet, off the ``*_api`` pools.
"""

from __future__ import annotations

import os

from dagster import AssetCheckResult, AssetCheckSeverity, asset_check

from grecohome_core.checks import alerting_check
from grecohome_core.silver import connect, list_payload_files
from grecohome_silver.config import settings
from grecohome_silver.dagster.strain_assets import STRAIN_PARQUET, silver_strain, strain_path
from grecohome_silver.strain import bronze_strain_count_sql


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


@asset_check(asset=silver_strain, name="strain_cycle_unique_nonnull")
@alerting_check
def strain_cycle_unique_nonnull() -> AssetCheckResult:
    """One row per ``cycle_id``; ``cycle_id`` and ``strain_date`` non-null."""
    path = strain_path(STRAIN_PARQUET)
    if not os.path.exists(path):
        return _missing(path)
    total = _scalar(f"SELECT count(*) FROM {_src(path)}")
    distinct = _scalar(f"SELECT count(DISTINCT cycle_id) FROM {_src(path)}")
    nulls = _scalar(
        f"SELECT count(*) FROM {_src(path)} WHERE cycle_id IS NULL OR strain_date IS NULL"
    )
    return AssetCheckResult(
        passed=(total == distinct and nulls == 0),
        severity=AssetCheckSeverity.ERROR,
        metadata={"rows": total, "distinct_cycles": distinct, "null_keys": nulls},
    )


@asset_check(asset=silver_strain, name="strain_value_ranges")
@alerting_check
def strain_value_ranges() -> AssetCheckResult:
    """Non-null metrics within plausible bounds (strain 0–21, HR 20–240, kJ ≥ 0)."""
    path = strain_path(STRAIN_PARQUET)
    if not os.path.exists(path):
        return _missing(path)
    clauses = [
        "(day_strain IS NOT NULL AND (day_strain < 0 OR day_strain > 21))",
        "(kilojoules IS NOT NULL AND kilojoules < 0)",
        "(avg_heart_rate IS NOT NULL AND (avg_heart_rate < 20 OR avg_heart_rate > 240))",
        "(max_heart_rate IS NOT NULL AND (max_heart_rate < 20 OR max_heart_rate > 240))",
    ]
    bad = _scalar(f"SELECT count(*) FROM {_src(path)} WHERE {' OR '.join(clauses)}")
    return AssetCheckResult(
        passed=(bad == 0),
        severity=AssetCheckSeverity.ERROR,
        metadata={"out_of_range_rows": bad},
    )


@asset_check(asset=silver_strain, name="strain_coverage_vs_bronze")
@alerting_check
def strain_coverage_vs_bronze() -> AssetCheckResult:
    """silver cycles ≈ bronze distinct cycles — no silent drop."""
    path = strain_path(STRAIN_PARQUET)
    if not os.path.exists(path):
        return _missing(path)
    silver_rows = _scalar(f"SELECT count(*) FROM {_src(path)}")
    files = list_payload_files(settings.bronze_root, "whoop", "cycle")
    bronze_cycles = _scalar(bronze_strain_count_sql(files))
    return AssetCheckResult(
        passed=(silver_rows >= bronze_cycles),
        severity=AssetCheckSeverity.WARN,
        metadata={
            "silver_cycles": silver_rows,
            "bronze_distinct_cycles": bronze_cycles,
            "distinct_days": _scalar(f"SELECT count(DISTINCT strain_date) FROM {_src(path)}"),
        },
    )


STRAIN_CHECKS = [
    strain_cycle_unique_nonnull,
    strain_value_ranges,
    strain_coverage_vs_bronze,
]
