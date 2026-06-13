"""Asset checks for the gold daily weather mart.

Same convention as the wellness mart: structural correctness = ERROR; coverage = WARN.
Read-only over the written Parquet, off the ``*_api`` pools. Range bounds are generous
envelopes (imperial) sized to catch a join/aggregation/unit bug, not to police weather.
"""

from __future__ import annotations

import os

from dagster import AssetCheckResult, AssetCheckSeverity, asset_check

from grecohome_core.checks import alerting_check
from grecohome_core.silver import connect
from grecohome_gold.dagster.weather_assets import (
    WEATHER_PARQUET,
    gold_daily_weather,
    gold_weather_path,
)
from grecohome_gold.daily_weather import DEPTHS

# Air/surface temperature columns (°F).
_AIR_TEMP_F = ("air_temp_max_f", "air_temp_min_f", "air_temp_avg_f")
_SURFACE_TEMP_F = ("surface_temp_max_f", "surface_temp_min_f")
_SOIL_TEMP_F = tuple(f"soil_temp_{d}_f_mean" for d in DEPTHS)
_SOIL_MOISTURE = tuple(f"soil_moisture_{d}_mean" for d in DEPTHS)


def _missing(path: str) -> AssetCheckResult:
    return AssetCheckResult(
        passed=False,
        severity=AssetCheckSeverity.ERROR,
        metadata={"error": f"gold output missing: {path}"},
    )


def _scalar(sql: str) -> int:
    return int(connect().execute(sql).fetchone()[0])


def _src() -> str:
    path = gold_weather_path(WEATHER_PARQUET)
    return f"read_parquet('{path.replace(chr(39), chr(39) * 2)}')"


@asset_check(asset=gold_daily_weather, name="weather_day_unique_nonnull")
@alerting_check
def weather_day_unique_nonnull() -> AssetCheckResult:
    """One row per ``day``, never null — the spine must be a clean daily grain."""
    path = gold_weather_path(WEATHER_PARQUET)
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


@asset_check(asset=gold_daily_weather, name="weather_value_ranges")
@alerting_check
def weather_value_ranges() -> AssetCheckResult:
    """Aggregates within plausible bounds (catches an aggregation/unit bug)."""
    path = gold_weather_path(WEATHER_PARQUET)
    if not os.path.exists(path):
        return _missing(path)
    clauses = [f"({c} IS NOT NULL AND ({c} < -60 OR {c} > 200))" for c in _AIR_TEMP_F]
    clauses += [f"({c} IS NOT NULL AND ({c} < -60 OR {c} > 200))" for c in _SURFACE_TEMP_F]
    clauses += [f"({c} IS NOT NULL AND ({c} < -40 OR {c} > 140))" for c in _SOIL_TEMP_F]
    clauses += [f"({c} IS NOT NULL AND ({c} < 0 OR {c} > 1))" for c in _SOIL_MOISTURE]
    clauses += [
        "(rh_mean_pct IS NOT NULL AND (rh_mean_pct < 0 OR rh_mean_pct > 100))",
        "(precip_total_in IS NOT NULL AND precip_total_in < 0)",
        "(gdd50 IS NOT NULL AND gdd50 < 0)",
        "(solar_rad_mean_wm2 IS NOT NULL AND solar_rad_mean_wm2 < 0)",
        "(solar_rad_max_wm2 IS NOT NULL AND solar_rad_max_wm2 < 0)",
        "(hours_observed < 0 OR hours_observed > 26)",
        # Daily max can never be below daily min (a swapped-aggregate bug).
        "(air_temp_max_f IS NOT NULL AND air_temp_min_f IS NOT NULL "
        "AND air_temp_max_f < air_temp_min_f)",
    ]
    bad = _scalar(f"SELECT count(*) FROM {_src()} WHERE {' OR '.join(clauses)}")
    return AssetCheckResult(
        passed=(bad == 0),
        severity=AssetCheckSeverity.ERROR,
        metadata={"out_of_range_rows": bad},
    )


@asset_check(asset=gold_daily_weather, name="weather_coverage")
@alerting_check
def weather_coverage() -> AssetCheckResult:
    """Report day coverage; warn if no day carries weather (empty mart)."""
    path = gold_weather_path(WEATHER_PARQUET)
    if not os.path.exists(path):
        return _missing(path)
    total = _scalar(f"SELECT count(*) FROM {_src()}")
    with_weather = _scalar(f"SELECT count(*) FROM {_src()} WHERE has_weather")
    return AssetCheckResult(
        passed=(with_weather > 0),
        severity=AssetCheckSeverity.WARN,
        metadata={
            "days": total,
            "days_with_weather": with_weather,
            "frost_days": _scalar(f"SELECT count(*) FROM {_src()} WHERE frost"),
        },
    )


WEATHER_CHECKS = [weather_day_unique_nonnull, weather_value_ranges, weather_coverage]
