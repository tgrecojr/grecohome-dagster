"""Asset checks for the gold daily wellness mart.

Same convention as silver: structural correctness = ERROR; coverage = WARN. Read-only
over the written Parquet, off the ``*_api`` pools.
"""

from __future__ import annotations

import os

from dagster import AssetCheckResult, AssetCheckSeverity, asset_check

from grecohome_core.checks import alerting_check
from grecohome_core.silver import connect
from grecohome_gold.dagster.assets import WELLNESS_PARQUET, gold_daily_wellness, gold_path


def _missing(path: str) -> AssetCheckResult:
    return AssetCheckResult(
        passed=False,
        severity=AssetCheckSeverity.ERROR,
        metadata={"error": f"gold output missing: {path}"},
    )


def _scalar(sql: str) -> int:
    return int(connect().execute(sql).fetchone()[0])


def _src() -> str:
    path = gold_path(WELLNESS_PARQUET)
    return f"read_parquet('{path.replace(chr(39), chr(39) * 2)}')"


@asset_check(asset=gold_daily_wellness, name="wellness_day_unique_nonnull")
@alerting_check
def wellness_day_unique_nonnull() -> AssetCheckResult:
    """One row per ``day``, never null — the spine must be a clean daily grain."""
    path = gold_path(WELLNESS_PARQUET)
    if not os.path.exists(path):
        return _missing(path)
    total = _scalar(f"SELECT count(*) FROM {_src()}")
    distinct = _scalar(f"SELECT count(DISTINCT day) FROM {_src()}")
    nulls = _scalar(f"SELECT count(*) FROM {_src()} WHERE day IS NULL")
    return AssetCheckResult(
        passed=(total == distinct and nulls == 0),
        severity=AssetCheckSeverity.ERROR,
        metadata={"rows": total, "distinct_days": distinct, "null_days": nulls},
    )


@asset_check(asset=gold_daily_wellness, name="wellness_value_ranges")
@alerting_check
def wellness_value_ranges() -> AssetCheckResult:
    """Aggregates within plausible bounds (catches a join/aggregation bug)."""
    path = gold_path(WELLNESS_PARQUET)
    if not os.path.exists(path):
        return _missing(path)
    clauses = [
        "(glucose_tir_pct IS NOT NULL AND (glucose_tir_pct < 0 OR glucose_tir_pct > 100))",
        "(glucose_mean IS NOT NULL AND (glucose_mean < 10 OR glucose_mean > 600))",
        "(recovery_score IS NOT NULL AND (recovery_score < 0 OR recovery_score > 100))",
        "(workout_count < 0)",
        "(workout_total_min IS NOT NULL AND workout_total_min < 0)",
        "(glucose_readings IS NOT NULL AND glucose_readings <= 0)",
    ]
    bad = _scalar(f"SELECT count(*) FROM {_src()} WHERE {' OR '.join(clauses)}")
    return AssetCheckResult(
        passed=(bad == 0),
        severity=AssetCheckSeverity.ERROR,
        metadata={"out_of_range_rows": bad},
    )


@asset_check(asset=gold_daily_wellness, name="wellness_coverage")
@alerting_check
def wellness_coverage() -> AssetCheckResult:
    """Report per-source day coverage; warn if no day has any source (empty mart)."""
    path = gold_path(WELLNESS_PARQUET)
    if not os.path.exists(path):
        return _missing(path)
    total = _scalar(f"SELECT count(*) FROM {_src()}")
    any_src = _scalar(
        f"SELECT count(*) FROM {_src()} "
        "WHERE has_sleep OR has_recovery OR has_workout OR has_glucose"
    )
    return AssetCheckResult(
        passed=(any_src > 0),
        severity=AssetCheckSeverity.WARN,
        metadata={
            "days": total,
            "days_with_any_source": any_src,
            "days_with_sleep": _scalar(f"SELECT count(*) FROM {_src()} WHERE has_sleep"),
            "days_with_recovery": _scalar(f"SELECT count(*) FROM {_src()} WHERE has_recovery"),
            "days_with_workout": _scalar(f"SELECT count(*) FROM {_src()} WHERE has_workout"),
            "days_with_glucose": _scalar(f"SELECT count(*) FROM {_src()} WHERE has_glucose"),
        },
    )


ALL_CHECKS = [wellness_day_unique_nonnull, wellness_value_ranges, wellness_coverage]
