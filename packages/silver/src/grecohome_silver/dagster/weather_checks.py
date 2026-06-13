"""Silver asset checks for weather — same severity convention as the other silver checks.

Structural/dedup correctness = ERROR; coverage/expectation drift = WARN. Read-only over
the written Parquet, off the ``*_api`` pools. Range bounds are generous envelopes around
the live archive (air temp seen −19.6…37.3 °C, soil moisture 0.06…0.50, RH 11…100 %),
sized to catch a field-offset or unit bug, not to police the weather.
"""

from __future__ import annotations

import os

from dagster import AssetCheckResult, AssetCheckSeverity, asset_check

from grecohome_core.checks import alerting_check
from grecohome_core.silver import connect, list_payload_files
from grecohome_silver.config import settings
from grecohome_silver.dagster.weather_assets import WEATHER_PARQUET, silver_weather, weather_path
from grecohome_silver.weather import bronze_obs_count_sql

# Temperature columns (°C) — air, surface, and the five soil depths.
_TEMP_COLS = (
    "air_temp_c",
    "air_temp_max_c",
    "air_temp_min_c",
    "surface_temp_c",
    "surface_temp_max_c",
    "surface_temp_min_c",
    "soil_temp_5",
    "soil_temp_10",
    "soil_temp_20",
    "soil_temp_50",
    "soil_temp_100",
)
# Volumetric soil-moisture columns (m³/m³, a 0–1 fraction).
_MOISTURE_COLS = (
    "soil_moisture_5",
    "soil_moisture_10",
    "soil_moisture_20",
    "soil_moisture_50",
    "soil_moisture_100",
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


@asset_check(asset=silver_weather, name="weather_obs_unique_nonnull")
@alerting_check
def weather_obs_unique_nonnull() -> AssetCheckResult:
    """One row per observation (UTC instant); the UTC and local day keys are non-null."""
    path = weather_path(WEATHER_PARQUET)
    if not os.path.exists(path):
        return _missing(path)
    total = _scalar(f"SELECT count(*) FROM {_src(path)}")
    distinct = _scalar(f"SELECT count(DISTINCT obs_ts_utc) FROM {_src(path)}")
    nulls = _scalar(
        f"SELECT count(*) FROM {_src(path)} "
        "WHERE obs_ts_utc IS NULL OR obs_date_utc IS NULL OR obs_date_local IS NULL"
    )
    return AssetCheckResult(
        passed=(total == distinct and nulls == 0),
        severity=AssetCheckSeverity.ERROR,
        metadata={"rows": total, "distinct_obs": distinct, "null_keys": nulls},
    )


@asset_check(asset=silver_weather, name="weather_value_ranges")
@alerting_check
def weather_value_ranges() -> AssetCheckResult:
    """Non-null measurements within generous physical envelopes (catches a field/unit bug)."""
    path = weather_path(WEATHER_PARQUET)
    if not os.path.exists(path):
        return _missing(path)
    clauses = [f"({c} IS NOT NULL AND ({c} < -60 OR {c} > 60))" for c in _TEMP_COLS]
    clauses += [f"({c} IS NOT NULL AND ({c} < 0 OR {c} > 1))" for c in _MOISTURE_COLS]
    clauses += [
        "(rh_pct IS NOT NULL AND (rh_pct < 0 OR rh_pct > 100))",
        "(precip_mm IS NOT NULL AND precip_mm < 0)",
        "(solar_rad_wm2 IS NOT NULL AND (solar_rad_wm2 < 0 OR solar_rad_wm2 > 2000))",
    ]
    bad = _scalar(f"SELECT count(*) FROM {_src(path)} WHERE {' OR '.join(clauses)}")
    return AssetCheckResult(
        passed=(bad == 0),
        severity=AssetCheckSeverity.ERROR,
        metadata={"out_of_range_rows": bad},
    )


@asset_check(asset=silver_weather, name="weather_coverage_vs_bronze")
@alerting_check
def weather_coverage_vs_bronze() -> AssetCheckResult:
    """silver obs ≈ bronze distinct obs (UTC instants) — no silent drop."""
    path = weather_path(WEATHER_PARQUET)
    if not os.path.exists(path):
        return _missing(path)
    silver_rows = _scalar(f"SELECT count(*) FROM {_src(path)}")
    files = list_payload_files(settings.bronze_root, "uscrn", "hourly")
    bronze_obs = _scalar(bronze_obs_count_sql(files))
    return AssetCheckResult(
        passed=(silver_rows >= bronze_obs),
        severity=AssetCheckSeverity.WARN,
        metadata={
            "silver_obs": silver_rows,
            "bronze_distinct_obs": bronze_obs,
            "distinct_local_days": _scalar(
                f"SELECT count(DISTINCT obs_date_local) FROM {_src(path)}"
            ),
        },
    )


WEATHER_CHECKS = [
    weather_obs_unique_nonnull,
    weather_value_ranges,
    weather_coverage_vs_bronze,
]
