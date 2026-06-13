"""Silver asset checks for fitness — same severity convention as the other silver checks.

Structural correctness = ERROR; coverage = WARN. Read-only over the written Parquet, off the
``*_api`` pools. Coverage is expected to be **sparse** (snapshot endpoints, capture began
2026-06-03), so the coverage check only fails on a fully-empty table.
"""

from __future__ import annotations

import os

from dagster import AssetCheckResult, AssetCheckSeverity, asset_check

from grecohome_core.checks import alerting_check
from grecohome_core.silver import connect
from grecohome_silver.dagster.fitness_assets import FITNESS_PARQUET, fitness_path, silver_fitness


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


@asset_check(asset=silver_fitness, name="fitness_day_unique_nonnull")
@alerting_check
def fitness_day_unique_nonnull() -> AssetCheckResult:
    """One row per ``snapshot_date``, never null — a clean daily snapshot grain."""
    path = fitness_path(FITNESS_PARQUET)
    if not os.path.exists(path):
        return _missing(path)
    total = _scalar(f"SELECT count(*) FROM {_src(path)}")
    distinct = _scalar(f"SELECT count(DISTINCT snapshot_date) FROM {_src(path)}")
    nulls = _scalar(f"SELECT count(*) FROM {_src(path)} WHERE snapshot_date IS NULL")
    return AssetCheckResult(
        passed=(total == distinct and nulls == 0),
        severity=AssetCheckSeverity.ERROR,
        metadata={"rows": total, "distinct_days": distinct, "null_days": nulls},
    )


@asset_check(asset=silver_fitness, name="fitness_value_ranges")
@alerting_check
def fitness_value_ranges() -> AssetCheckResult:
    """Non-null metrics within plausible bounds (VO2max 10–90, race times > 0)."""
    path = fitness_path(FITNESS_PARQUET)
    if not os.path.exists(path):
        return _missing(path)
    clauses = [
        "(vo2max_running IS NOT NULL AND (vo2max_running < 10 OR vo2max_running > 90))",
        "(vo2max_cycling IS NOT NULL AND (vo2max_cycling < 10 OR vo2max_cycling > 90))",
        "(weekly_training_load IS NOT NULL AND weekly_training_load < 0)",
        "(race_5k_sec IS NOT NULL AND race_5k_sec <= 0)",
        "(race_marathon_sec IS NOT NULL AND race_marathon_sec <= 0)",
    ]
    bad = _scalar(f"SELECT count(*) FROM {_src(path)} WHERE {' OR '.join(clauses)}")
    return AssetCheckResult(
        passed=(bad == 0),
        severity=AssetCheckSeverity.ERROR,
        metadata={"out_of_range_rows": bad},
    )


@asset_check(asset=silver_fitness, name="fitness_coverage")
@alerting_check
def fitness_coverage() -> AssetCheckResult:
    """Report per-metric coverage; warn only if the table is empty (sparse is expected)."""
    path = fitness_path(FITNESS_PARQUET)
    if not os.path.exists(path):
        return _missing(path)
    total = _scalar(f"SELECT count(*) FROM {_src(path)}")
    return AssetCheckResult(
        passed=(total > 0),
        severity=AssetCheckSeverity.WARN,
        metadata={
            "snapshot_days": total,
            "days_with_vo2max": _scalar(
                f"SELECT count(*) FROM {_src(path)} "
                "WHERE vo2max_running IS NOT NULL OR vo2max_cycling IS NOT NULL"
            ),
            "days_with_status": _scalar(
                f"SELECT count(*) FROM {_src(path)} WHERE training_status_code IS NOT NULL"
            ),
            "days_with_race": _scalar(
                f"SELECT count(*) FROM {_src(path)} WHERE race_5k_sec IS NOT NULL"
            ),
        },
    )


FITNESS_CHECKS = [
    fitness_day_unique_nonnull,
    fitness_value_ranges,
    fitness_coverage,
]
