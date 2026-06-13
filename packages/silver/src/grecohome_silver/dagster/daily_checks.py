"""Silver asset checks for the daily summary — same severity convention as the others.

Structural/dedup correctness = ERROR; coverage/expectation drift = WARN. Read-only over
the written Parquet, off the ``*_api`` pools. Bounds are generous envelopes sized to catch
a field/unit bug, not to police the data.
"""

from __future__ import annotations

import os

from dagster import AssetCheckResult, AssetCheckSeverity, asset_check

from grecohome_core.checks import alerting_check
from grecohome_core.silver import connect, list_payload_files
from grecohome_silver.config import settings
from grecohome_silver.dagster.daily_assets import DAILY_PARQUET, daily_path, silver_daily
from grecohome_silver.daily import bronze_day_count_sql


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


@asset_check(asset=silver_daily, name="daily_date_unique_nonnull")
@alerting_check
def daily_date_unique_nonnull() -> AssetCheckResult:
    """One row per ``activity_date``, never null — a clean daily grain."""
    path = daily_path(DAILY_PARQUET)
    if not os.path.exists(path):
        return _missing(path)
    total = _scalar(f"SELECT count(*) FROM {_src(path)}")
    distinct = _scalar(f"SELECT count(DISTINCT activity_date) FROM {_src(path)}")
    nulls = _scalar(f"SELECT count(*) FROM {_src(path)} WHERE activity_date IS NULL")
    return AssetCheckResult(
        passed=(total == distinct and nulls == 0),
        severity=AssetCheckSeverity.ERROR,
        metadata={"rows": total, "distinct_days": distinct, "null_dates": nulls},
    )


@asset_check(asset=silver_daily, name="daily_value_ranges")
@alerting_check
def daily_value_ranges() -> AssetCheckResult:
    """Non-null metrics within generous bounds (catches a field/unit bug)."""
    path = daily_path(DAILY_PARQUET)
    if not os.path.exists(path):
        return _missing(path)
    clauses = [
        "(total_steps IS NOT NULL AND (total_steps < 0 OR total_steps > 200000))",
        "(total_distance_m IS NOT NULL AND total_distance_m < 0)",
        "(active_kilocalories IS NOT NULL AND (active_kilocalories < 0 "
        "OR active_kilocalories > 30000))",
        "(resting_heart_rate IS NOT NULL AND (resting_heart_rate < 20 "
        "OR resting_heart_rate > 150))",
        "(max_heart_rate IS NOT NULL AND (max_heart_rate < 20 OR max_heart_rate > 240))",
        "(avg_stress_level IS NOT NULL AND (avg_stress_level < 0 OR avg_stress_level > 100))",
        "(avg_spo2 IS NOT NULL AND (avg_spo2 < 50 OR avg_spo2 > 100))",
        "(body_battery_high IS NOT NULL AND (body_battery_high < 0 OR body_battery_high > 100))",
    ]
    bad = _scalar(f"SELECT count(*) FROM {_src(path)} WHERE {' OR '.join(clauses)}")
    return AssetCheckResult(
        passed=(bad == 0),
        severity=AssetCheckSeverity.ERROR,
        metadata={"out_of_range_rows": bad},
    )


@asset_check(asset=silver_daily, name="daily_coverage_vs_bronze")
@alerting_check
def daily_coverage_vs_bronze() -> AssetCheckResult:
    """silver days ≈ bronze distinct days — no silent drop."""
    path = daily_path(DAILY_PARQUET)
    if not os.path.exists(path):
        return _missing(path)
    silver_rows = _scalar(f"SELECT count(*) FROM {_src(path)}")
    files = list_payload_files(settings.bronze_root, "garmin", "user_summary")
    bronze_days = _scalar(bronze_day_count_sql(files))
    return AssetCheckResult(
        passed=(silver_rows >= bronze_days),
        severity=AssetCheckSeverity.WARN,
        metadata={
            "silver_days": silver_rows,
            "bronze_distinct_days": bronze_days,
            "days_with_steps": _scalar(
                f"SELECT count(*) FROM {_src(path)} WHERE total_steps IS NOT NULL"
            ),
        },
    )


DAILY_CHECKS = [
    daily_date_unique_nonnull,
    daily_value_ranges,
    daily_coverage_vs_bronze,
]
