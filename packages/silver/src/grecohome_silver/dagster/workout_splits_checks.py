"""Silver asset checks for workout splits — same severity convention as the other checks.

Structural/dedup correctness = ERROR; coverage/expectation drift = WARN. Read-only over the
written Parquet, off the ``*_api`` pools.
"""

from __future__ import annotations

import os

from dagster import AssetCheckResult, AssetCheckSeverity, asset_check

from grecohome_core.checks import alerting_check
from grecohome_core.silver import connect, list_payload_files
from grecohome_silver.config import settings
from grecohome_silver.dagster.workout_splits_assets import (
    SPLITS_PARQUET,
    silver_workout_splits,
    splits_path,
)
from grecohome_silver.workout_splits import bronze_lap_count_sql


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


@asset_check(asset=silver_workout_splits, name="splits_lap_unique_nonnull")
@alerting_check
def splits_lap_unique_nonnull() -> AssetCheckResult:
    """One row per (activity_id, lap_index); both keys non-null."""
    path = splits_path(SPLITS_PARQUET)
    if not os.path.exists(path):
        return _missing(path)
    total = _scalar(f"SELECT count(*) FROM {_src(path)}")
    distinct = _scalar(
        f"SELECT count(DISTINCT (activity_id, lap_index)) FROM {_src(path)}"
    )
    nulls = _scalar(
        f"SELECT count(*) FROM {_src(path)} WHERE activity_id IS NULL OR lap_index IS NULL"
    )
    return AssetCheckResult(
        passed=(total == distinct and nulls == 0),
        severity=AssetCheckSeverity.ERROR,
        metadata={"rows": total, "distinct_laps": distinct, "null_keys": nulls},
    )


@asset_check(asset=silver_workout_splits, name="splits_value_ranges")
@alerting_check
def splits_value_ranges() -> AssetCheckResult:
    """Non-null metrics within plausible bounds (durations/distance ≥ 0, HR 0–240)."""
    path = splits_path(SPLITS_PARQUET)
    if not os.path.exists(path):
        return _missing(path)
    clauses = [
        "(duration_sec IS NOT NULL AND duration_sec < 0)",
        "(distance_m IS NOT NULL AND distance_m < 0)",
        "(avg_speed_mps IS NOT NULL AND avg_speed_mps < 0)",
        "(avg_hr IS NOT NULL AND (avg_hr < 0 OR avg_hr > 240))",
        "(max_hr IS NOT NULL AND (max_hr < 0 OR max_hr > 240))",
        "(lap_index IS NOT NULL AND lap_index < 0)",
    ]
    bad = _scalar(f"SELECT count(*) FROM {_src(path)} WHERE {' OR '.join(clauses)}")
    return AssetCheckResult(
        passed=(bad == 0),
        severity=AssetCheckSeverity.ERROR,
        metadata={"out_of_range_rows": bad},
    )


@asset_check(asset=silver_workout_splits, name="splits_coverage_vs_bronze")
@alerting_check
def splits_coverage_vs_bronze() -> AssetCheckResult:
    """silver laps ≈ bronze distinct laps — no silent drop."""
    path = splits_path(SPLITS_PARQUET)
    if not os.path.exists(path):
        return _missing(path)
    silver_rows = _scalar(f"SELECT count(*) FROM {_src(path)}")
    files = list_payload_files(settings.bronze_root, "garmin", "activity_splits")
    bronze_laps = _scalar(bronze_lap_count_sql(files))
    return AssetCheckResult(
        passed=(silver_rows >= bronze_laps),
        severity=AssetCheckSeverity.WARN,
        metadata={
            "silver_laps": silver_rows,
            "bronze_distinct_laps": bronze_laps,
            "distinct_activities": _scalar(
                f"SELECT count(DISTINCT activity_id) FROM {_src(path)}"
            ),
        },
    )


WORKOUT_SPLITS_CHECKS = [
    splits_lap_unique_nonnull,
    splits_value_ranges,
    splits_coverage_vs_bronze,
]
