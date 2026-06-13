"""Silver asset checks for Whoop workouts — same severity convention as the other checks.

Structural/dedup correctness = ERROR; coverage/expectation drift = WARN. Read-only over the
written Parquet, off the ``*_api`` pools.
"""

from __future__ import annotations

import os

from dagster import AssetCheckResult, AssetCheckSeverity, asset_check

from grecohome_core.checks import alerting_check
from grecohome_core.silver import connect, list_payload_files
from grecohome_silver.config import settings
from grecohome_silver.dagster.whoop_workouts_assets import (
    WHOOP_WORKOUTS_PARQUET,
    silver_whoop_workouts,
    whoop_workouts_path,
)
from grecohome_silver.whoop_workouts import bronze_workout_count_sql


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


@asset_check(asset=silver_whoop_workouts, name="whoop_workout_unique_nonnull")
@alerting_check
def whoop_workout_unique_nonnull() -> AssetCheckResult:
    """One row per ``workout_id``; ``workout_id`` and ``workout_date`` non-null."""
    path = whoop_workouts_path(WHOOP_WORKOUTS_PARQUET)
    if not os.path.exists(path):
        return _missing(path)
    total = _scalar(f"SELECT count(*) FROM {_src(path)}")
    distinct = _scalar(f"SELECT count(DISTINCT workout_id) FROM {_src(path)}")
    nulls = _scalar(
        f"SELECT count(*) FROM {_src(path)} WHERE workout_id IS NULL OR workout_date IS NULL"
    )
    return AssetCheckResult(
        passed=(total == distinct and nulls == 0),
        severity=AssetCheckSeverity.ERROR,
        metadata={"rows": total, "distinct_workouts": distinct, "null_keys": nulls},
    )


@asset_check(asset=silver_whoop_workouts, name="whoop_workout_value_ranges")
@alerting_check
def whoop_workout_value_ranges() -> AssetCheckResult:
    """Non-null metrics within plausible bounds (strain 0–21, HR 20–240, distance ≥ 0)."""
    path = whoop_workouts_path(WHOOP_WORKOUTS_PARQUET)
    if not os.path.exists(path):
        return _missing(path)
    clauses = [
        "(strain IS NOT NULL AND (strain < 0 OR strain > 21))",
        "(avg_heart_rate IS NOT NULL AND (avg_heart_rate < 20 OR avg_heart_rate > 240))",
        "(max_heart_rate IS NOT NULL AND (max_heart_rate < 20 OR max_heart_rate > 240))",
        "(kilojoules IS NOT NULL AND kilojoules < 0)",
        "(distance_m IS NOT NULL AND distance_m < 0)",
    ]
    bad = _scalar(f"SELECT count(*) FROM {_src(path)} WHERE {' OR '.join(clauses)}")
    return AssetCheckResult(
        passed=(bad == 0),
        severity=AssetCheckSeverity.ERROR,
        metadata={"out_of_range_rows": bad},
    )


@asset_check(asset=silver_whoop_workouts, name="whoop_workout_coverage_vs_bronze")
@alerting_check
def whoop_workout_coverage_vs_bronze() -> AssetCheckResult:
    """silver workouts ≈ bronze distinct workouts — no silent drop."""
    path = whoop_workouts_path(WHOOP_WORKOUTS_PARQUET)
    if not os.path.exists(path):
        return _missing(path)
    silver_rows = _scalar(f"SELECT count(*) FROM {_src(path)}")
    files = list_payload_files(settings.bronze_root, "whoop", "workout")
    bronze_workouts = _scalar(bronze_workout_count_sql(files))
    return AssetCheckResult(
        passed=(silver_rows >= bronze_workouts),
        severity=AssetCheckSeverity.WARN,
        metadata={
            "silver_workouts": silver_rows,
            "bronze_distinct_workouts": bronze_workouts,
            "distinct_sports": _scalar(f"SELECT count(DISTINCT sport_name) FROM {_src(path)}"),
        },
    )


WHOOP_WORKOUTS_CHECKS = [
    whoop_workout_unique_nonnull,
    whoop_workout_value_ranges,
    whoop_workout_coverage_vs_bronze,
]
