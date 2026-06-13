"""Silver asset checks for body/weigh-ins — same severity convention as the other checks.

Structural/dedup correctness = ERROR; coverage/expectation drift = WARN. Read-only over the
written Parquet, off the ``*_api`` pools.
"""

from __future__ import annotations

import os

from dagster import AssetCheckResult, AssetCheckSeverity, asset_check

from grecohome_core.checks import alerting_check
from grecohome_core.silver import connect, list_payload_files
from grecohome_silver.body import bronze_weighin_count_sql
from grecohome_silver.config import settings
from grecohome_silver.dagster.body_assets import BODY_PARQUET, body_path, silver_body


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


@asset_check(asset=silver_body, name="body_sample_unique_nonnull")
@alerting_check
def body_sample_unique_nonnull() -> AssetCheckResult:
    """One row per ``sample_pk``; ``sample_pk`` and ``measured_date`` non-null."""
    path = body_path(BODY_PARQUET)
    if not os.path.exists(path):
        return _missing(path)
    total = _scalar(f"SELECT count(*) FROM {_src(path)}")
    distinct = _scalar(f"SELECT count(DISTINCT sample_pk) FROM {_src(path)}")
    nulls = _scalar(
        f"SELECT count(*) FROM {_src(path)} WHERE sample_pk IS NULL OR measured_date IS NULL"
    )
    return AssetCheckResult(
        passed=(total == distinct and nulls == 0),
        severity=AssetCheckSeverity.ERROR,
        metadata={"rows": total, "distinct_weighins": distinct, "null_keys": nulls},
    )


@asset_check(asset=silver_body, name="body_value_ranges")
@alerting_check
def body_value_ranges() -> AssetCheckResult:
    """Non-null metrics within plausible bounds (catches a unit bug — esp. grams vs kg)."""
    path = body_path(BODY_PARQUET)
    if not os.path.exists(path):
        return _missing(path)
    clauses = [
        "(weight_kg IS NOT NULL AND (weight_kg < 20 OR weight_kg > 300))",
        "(bmi IS NOT NULL AND (bmi < 10 OR bmi > 80))",
        "(body_fat_pct IS NOT NULL AND (body_fat_pct < 0 OR body_fat_pct > 70))",
        "(body_water_pct IS NOT NULL AND (body_water_pct < 0 OR body_water_pct > 100))",
        "(muscle_mass_kg IS NOT NULL AND (muscle_mass_kg < 0 OR muscle_mass_kg > 200))",
    ]
    bad = _scalar(f"SELECT count(*) FROM {_src(path)} WHERE {' OR '.join(clauses)}")
    return AssetCheckResult(
        passed=(bad == 0),
        severity=AssetCheckSeverity.ERROR,
        metadata={"out_of_range_rows": bad},
    )


@asset_check(asset=silver_body, name="body_coverage_vs_bronze")
@alerting_check
def body_coverage_vs_bronze() -> AssetCheckResult:
    """silver weigh-ins ≈ bronze distinct weigh-ins — no silent drop."""
    path = body_path(BODY_PARQUET)
    if not os.path.exists(path):
        return _missing(path)
    silver_rows = _scalar(f"SELECT count(*) FROM {_src(path)}")
    files = list_payload_files(settings.bronze_root, "garmin", "daily_weigh_ins")
    bronze_weighins = _scalar(bronze_weighin_count_sql(files))
    return AssetCheckResult(
        passed=(silver_rows >= bronze_weighins),
        severity=AssetCheckSeverity.WARN,
        metadata={
            "silver_weighins": silver_rows,
            "bronze_distinct_weighins": bronze_weighins,
            "distinct_days": _scalar(f"SELECT count(DISTINCT measured_date) FROM {_src(path)}"),
        },
    )


BODY_CHECKS = [
    body_sample_unique_nonnull,
    body_value_ranges,
    body_coverage_vs_bronze,
]
