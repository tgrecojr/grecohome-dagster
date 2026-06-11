"""Silver asset checks for workouts — same severity convention as the other tables.

Structural/dedup correctness = ERROR; coverage = WARN. Read-only over the written
Parquet, off the ``*_api`` pools.
"""

from __future__ import annotations

import os

from dagster import AssetCheckResult, AssetCheckSeverity, asset_check

from grecohome_core.silver import connect, list_payload_files
from grecohome_silver.config import settings
from grecohome_silver.dagster.workout_assets import (
    WORKOUTS_PARQUET,
    silver_workouts,
    workouts_path,
)
from grecohome_silver.workouts import bronze_activity_count_sql

# Range-check columns (seconds; metres; bpm). avg_hr/max_hr of 0 means "no HR
# recorded" and is allowed; only physiologically impossible values fail.
_NONNEG_COLS = (
    "duration_sec",
    "moving_duration_sec",
    "elapsed_duration_sec",
    "distance_m",
    "calories",
)


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


@asset_check(asset=silver_workouts, name="workouts_id_unique_nonnull")
def workouts_id_unique_nonnull() -> AssetCheckResult:
    """One row per ``activity_id``; ``activity_id`` and ``activity_date`` non-null."""
    path = workouts_path(WORKOUTS_PARQUET)
    if not os.path.exists(path):
        return _missing(path)
    total = _scalar(f"SELECT count(*) FROM {_src(path)}")
    distinct = _scalar(f"SELECT count(DISTINCT activity_id) FROM {_src(path)}")
    nulls = _scalar(
        f"SELECT count(*) FROM {_src(path)} WHERE activity_id IS NULL OR activity_date IS NULL"
    )
    return AssetCheckResult(
        passed=(total == distinct and nulls == 0),
        severity=AssetCheckSeverity.ERROR,
        metadata={"rows": total, "distinct_activities": distinct, "null_keys": nulls},
    )


@asset_check(asset=silver_workouts, name="workouts_value_ranges")
def workouts_value_ranges() -> AssetCheckResult:
    """Durations/distance/calories ≥ 0 and bounded; HR within 0–240 (0 = not recorded)."""
    path = workouts_path(WORKOUTS_PARQUET)
    if not os.path.exists(path):
        return _missing(path)
    clauses = [f"({c} IS NOT NULL AND {c} < 0)" for c in _NONNEG_COLS]
    clauses.append("(duration_sec IS NOT NULL AND duration_sec >= 604800)")  # < 7 days
    clauses.append("(distance_m IS NOT NULL AND distance_m >= 1000000)")  # < 1000 km
    clauses += [f"({c} IS NOT NULL AND ({c} < 0 OR {c} > 240))" for c in ("avg_hr", "max_hr")]
    bad = _scalar(f"SELECT count(*) FROM {_src(path)} WHERE {' OR '.join(clauses)}")
    return AssetCheckResult(
        passed=(bad == 0),
        severity=AssetCheckSeverity.ERROR,
        metadata={"out_of_range_rows": bad},
    )


@asset_check(asset=silver_workouts, name="workouts_coverage_vs_bronze")
def workouts_coverage_vs_bronze() -> AssetCheckResult:
    """silver activities ≈ bronze distinct ``activityId`` — no silent drop."""
    path = workouts_path(WORKOUTS_PARQUET)
    if not os.path.exists(path):
        return _missing(path)
    silver_rows = _scalar(f"SELECT count(*) FROM {_src(path)}")
    files = list_payload_files(settings.bronze_root, "garmin", "activities")
    bronze_activities = _scalar(bronze_activity_count_sql(files))
    return AssetCheckResult(
        passed=(silver_rows >= bronze_activities),
        severity=AssetCheckSeverity.WARN,
        metadata={
            "silver_activities": silver_rows,
            "bronze_distinct_activities": bronze_activities,
        },
    )


WORKOUT_CHECKS = [
    workouts_id_unique_nonnull,
    workouts_value_ranges,
    workouts_coverage_vs_bronze,
]
