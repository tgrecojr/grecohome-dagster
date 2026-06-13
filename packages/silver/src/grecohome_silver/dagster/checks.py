"""Silver asset checks — the bronze-check pattern, applied to silver Parquet.

Severities mirror the bronze convention: structural/parse/dedup correctness = ERROR
(the whole point of silver is one typed row per logical record); coverage and
expectation drift = WARN (visible each run, not run-blocking). Checks read the
written Parquet read-only and carry **no concurrency pool**, so they never contend
with the ``*_api`` ingestion pools (silver makes no source calls).
"""

from __future__ import annotations

import os

from dagster import AssetCheckResult, AssetCheckSeverity, asset_check

from grecohome_core.checks import alerting_check
from grecohome_core.silver import (
    connect,
    json_date,
    list_payload_files,
    payloads_relation_sql,
)
from grecohome_silver.config import settings
from grecohome_silver.dagster.assets import (
    GARMIN_PARQUET,
    UNIFIED_PARQUET,
    WHOOP_PARQUET,
    silver_path,
    silver_sleep,
    silver_sleep_garmin,
    silver_sleep_whoop,
)

WHOOP_OWNED_SINCE = "2025-12-18"  # user acquired the Whoop; no whoop_* before this.

# Columns range-checked in silver_sleep: percentages 0..100, stage minutes 0..<24h.
_PCT_COLS = (
    "garmin_sleep_score",
    "whoop_performance_pct",
    "whoop_efficiency_pct",
    "whoop_consistency_pct",
)
_MIN_COLS = (
    "garmin_total_min",
    "garmin_deep_min",
    "garmin_light_min",
    "garmin_rem_min",
    "garmin_awake_min",
    "whoop_deep_min",
    "whoop_rem_min",
    "whoop_light_min",
    "whoop_awake_min",
)


def _missing(path: str) -> AssetCheckResult:
    """A uniform ERROR result when an asset's Parquet is not on disk yet."""
    return AssetCheckResult(
        passed=False,
        severity=AssetCheckSeverity.ERROR,
        metadata={"error": f"silver output missing: {path}"},
    )


def _scalar(sql: str) -> int:
    return int(connect().execute(sql).fetchone()[0])


def _src(path: str) -> str:
    return f"read_parquet('{path.replace(chr(39), chr(39) * 2)}')"


# ---------------------------------------------------------------------------
# Uniqueness + non-null key (ERROR) — dedup correctness
# ---------------------------------------------------------------------------
@asset_check(asset=silver_sleep_garmin, name="garmin_night_unique_nonnull")
@alerting_check
def garmin_night_unique_nonnull() -> AssetCheckResult:
    """One row per ``night_date``, never null, in silver_sleep_garmin."""
    path = silver_path(GARMIN_PARQUET)
    if not os.path.exists(path):
        return _missing(path)
    total = _scalar(f"SELECT count(*) FROM {_src(path)}")
    distinct = _scalar(f"SELECT count(DISTINCT night_date) FROM {_src(path)}")
    nulls = _scalar(f"SELECT count(*) FROM {_src(path)} WHERE night_date IS NULL")
    return AssetCheckResult(
        passed=(total == distinct and nulls == 0),
        severity=AssetCheckSeverity.ERROR,
        metadata={"rows": total, "distinct_nights": distinct, "null_nights": nulls},
    )


@asset_check(asset=silver_sleep_whoop, name="whoop_id_unique_night_nonnull")
@alerting_check
def whoop_id_unique_night_nonnull() -> AssetCheckResult:
    """One row per ``whoop_sleep_id``; ``night_date`` never null."""
    path = silver_path(WHOOP_PARQUET)
    if not os.path.exists(path):
        return _missing(path)
    total = _scalar(f"SELECT count(*) FROM {_src(path)}")
    distinct = _scalar(f"SELECT count(DISTINCT whoop_sleep_id) FROM {_src(path)}")
    nulls = _scalar(f"SELECT count(*) FROM {_src(path)} WHERE night_date IS NULL")
    return AssetCheckResult(
        passed=(total == distinct and nulls == 0),
        severity=AssetCheckSeverity.ERROR,
        metadata={"rows": total, "distinct_ids": distinct, "null_nights": nulls},
    )


@asset_check(asset=silver_sleep, name="sleep_night_unique_nonnull")
@alerting_check
def sleep_night_unique_nonnull() -> AssetCheckResult:
    """One row per ``night_date`` in the unified table, never null (the whole point)."""
    path = silver_path(UNIFIED_PARQUET)
    if not os.path.exists(path):
        return _missing(path)
    total = _scalar(f"SELECT count(*) FROM {_src(path)}")
    distinct = _scalar(f"SELECT count(DISTINCT night_date) FROM {_src(path)}")
    nulls = _scalar(f"SELECT count(*) FROM {_src(path)} WHERE night_date IS NULL")
    return AssetCheckResult(
        passed=(total == distinct and nulls == 0),
        severity=AssetCheckSeverity.ERROR,
        metadata={"rows": total, "distinct_nights": distinct, "null_nights": nulls},
    )


# ---------------------------------------------------------------------------
# Range validity (ERROR) — catches a parsing/unit bug
# ---------------------------------------------------------------------------
@asset_check(asset=silver_sleep, name="sleep_value_ranges")
@alerting_check
def sleep_value_ranges() -> AssetCheckResult:
    """Percentages 0–100; stage minutes ≥ 0 and < 24h. A breach means a unit bug."""
    path = silver_path(UNIFIED_PARQUET)
    if not os.path.exists(path):
        return _missing(path)
    clauses = [
        f"({c} IS NOT NULL AND ({c} < 0 OR {c} > 100))" for c in _PCT_COLS
    ] + [f"({c} IS NOT NULL AND ({c} < 0 OR {c} >= 1440))" for c in _MIN_COLS]
    bad = _scalar(f"SELECT count(*) FROM {_src(path)} WHERE {' OR '.join(clauses)}")
    return AssetCheckResult(
        passed=(bad == 0),
        severity=AssetCheckSeverity.ERROR,
        metadata={"out_of_range_rows": bad},
    )


# ---------------------------------------------------------------------------
# Join sanity (WARN) — no fully-null rows; recent single-source soft flag
# ---------------------------------------------------------------------------
@asset_check(asset=silver_sleep, name="sleep_join_sanity")
@alerting_check
def sleep_join_sanity() -> AssetCheckResult:
    """No row lacks both sources; recent single-source nights are a soft flag.

    Since the user wears both devices on recent nights, a night ≥ 2025-12-18 with
    only one source may signal a sync/wear gap on the other — surfaced (not failed).
    """
    path = silver_path(UNIFIED_PARQUET)
    if not os.path.exists(path):
        return _missing(path)
    fully_null = _scalar(
        f"SELECT count(*) FROM {_src(path)} WHERE NOT (has_garmin OR has_whoop)"
    )
    recent_single = _scalar(
        f"SELECT count(*) FROM {_src(path)} "
        f"WHERE night_date >= DATE '{WHOOP_OWNED_SINCE}' AND (has_garmin != has_whoop)"
    )
    return AssetCheckResult(
        passed=(fully_null == 0),
        severity=AssetCheckSeverity.WARN,
        metadata={"fully_null_rows": fully_null, "recent_single_source_nights": recent_single},
    )


# ---------------------------------------------------------------------------
# Coverage split (WARN) — make the source mix visible each run
# ---------------------------------------------------------------------------
@asset_check(asset=silver_sleep, name="sleep_coverage_split")
@alerting_check
def sleep_coverage_split() -> AssetCheckResult:
    """Report garmin-only / whoop-only / both night counts (a shift signals trouble)."""
    path = silver_path(UNIFIED_PARQUET)
    if not os.path.exists(path):
        return _missing(path)
    both = _scalar(f"SELECT count(*) FROM {_src(path)} WHERE has_garmin AND has_whoop")
    g_only = _scalar(f"SELECT count(*) FROM {_src(path)} WHERE has_garmin AND NOT has_whoop")
    w_only = _scalar(f"SELECT count(*) FROM {_src(path)} WHERE has_whoop AND NOT has_garmin")
    return AssetCheckResult(
        passed=True,
        severity=AssetCheckSeverity.WARN,
        metadata={"both": both, "garmin_only": g_only, "whoop_only": w_only},
    )


# ---------------------------------------------------------------------------
# Coverage vs bronze (WARN) — a big drop means dedup/filter is wrong
# ---------------------------------------------------------------------------
@asset_check(asset=silver_sleep_garmin, name="garmin_coverage_vs_bronze")
@alerting_check
def garmin_coverage_vs_bronze() -> AssetCheckResult:
    """silver_sleep_garmin nights ≈ bronze distinct ``calendarDate`` (no silent drop)."""
    path = silver_path(GARMIN_PARQUET)
    if not os.path.exists(path):
        return _missing(path)
    silver_rows = _scalar(f"SELECT count(*) FROM {_src(path)}")
    files = list_payload_files(settings.bronze_root, "garmin", "sleep")
    nights_expr = json_date("j", "dailySleepDTO.calendarDate")
    bronze_nights = _scalar(
        f"SELECT count(DISTINCT {nights_expr}) FROM ({payloads_relation_sql(files)}) "
        f"WHERE {nights_expr} IS NOT NULL"
    )
    return AssetCheckResult(
        passed=(silver_rows >= bronze_nights),
        severity=AssetCheckSeverity.WARN,
        metadata={"silver_nights": silver_rows, "bronze_distinct_nights": bronze_nights},
    )


SLEEP_CHECKS = [
    garmin_night_unique_nonnull,
    whoop_id_unique_night_nonnull,
    sleep_night_unique_nonnull,
    sleep_value_ranges,
    sleep_join_sanity,
    sleep_coverage_split,
    garmin_coverage_vs_bronze,
]
