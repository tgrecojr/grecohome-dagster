"""The daily weather mart — one row per local day, rolled up from ``silver_weather``.

The gardening-facing analysis layer: silver carries faithful hourly SI observations;
gold aggregates them to the gardener's **local day** and exposes the imperial + derived
metrics other applications consume:

* **temperature** — daily max / min / mean in °F;
* **growing-degree-days** — ``gdd50 = max(0, (Tmax_f + Tmin_f)/2 − 50)``, the base-50°F
  GDD used for plant-development tracking;
* **frost / hard-freeze flags** — daily min ≤ 32 °F / ≤ 28 °F;
* **precipitation** — daily total in inches;
* **solar** — mean / max W/m²;
* **soil** — mean temperature (°F) and mean volumetric moisture at each of the five
  depths (5/10/20/50/100 cm);
* **humidity** — daily mean RH %;
* **coverage** — hours observed + a ``has_weather`` provenance flag.

A continuous daily spine (min→max local day) makes gaps explicit for rolling/streak
analysis, mirroring the daily wellness mart. Rebuildable: the asset overwrites the mart
from current silver each run.
"""

from __future__ import annotations

import os

# Soil sensor depths (cm) present in every USCRN row.
DEPTHS = (5, 10, 20, 50, 100)

# Growing-degree-day base and freeze thresholds (°F).
GDD_BASE_F = 50
FROST_F = 32
HARD_FREEZE_F = 28


def _pq(silver_root: str, *parts: str) -> str:
    """A ``read_parquet('<silver_root>/.../x.parquet')`` source (single-quote escaped)."""
    path = os.path.join(silver_root, *parts)
    return f"read_parquet('{path.replace(chr(39), chr(39) * 2)}')"


def _f(celsius_expr: str) -> str:
    """Celsius expression → Fahrenheit."""
    return f"({celsius_expr}) * 9.0 / 5.0 + 32.0"


def daily_weather_sql(
    silver_root: str,
    *,
    gdd_base_f: int = GDD_BASE_F,
    frost_f: int = FROST_F,
    hard_freeze_f: int = HARD_FREEZE_F,
) -> str:
    """SQL for the daily weather mart over ``silver_weather`` under ``silver_root``."""
    weather = _pq(silver_root, "weather", "silver_weather.parquet")
    tmax_f = _f("max(air_temp_max_c)")
    tmin_f = _f("min(air_temp_min_c)")
    soil_temp_cols = ",\n                ".join(
        f"{_f(f'avg(soil_temp_{d})')} AS soil_temp_{d}_f_mean" for d in DEPTHS
    )
    soil_moisture_cols = ",\n                ".join(
        f"avg(soil_moisture_{d}) AS soil_moisture_{d}_mean" for d in DEPTHS
    )
    return f"""
        WITH agg AS (
            SELECT
                obs_date_local                         AS day,
                {tmax_f}                               AS air_temp_max_f,
                {tmin_f}                               AS air_temp_min_f,
                {_f("avg(air_temp_c)")}                AS air_temp_avg_f,
                greatest(0, ({tmax_f} + {tmin_f}) / 2 - {gdd_base_f}) AS gdd50,
                ({tmin_f} <= {frost_f})                AS frost,
                ({tmin_f} <= {hard_freeze_f})          AS hard_freeze,
                sum(precip_mm) / 25.4                  AS precip_total_in,
                avg(solar_rad_wm2)                     AS solar_rad_mean_wm2,
                max(solar_rad_wm2)                     AS solar_rad_max_wm2,
                {_f("max(surface_temp_max_c)")}        AS surface_temp_max_f,
                {_f("min(surface_temp_min_c)")}        AS surface_temp_min_f,
                avg(rh_pct)                            AS rh_mean_pct,
                {soil_temp_cols},
                {soil_moisture_cols},
                count(*)                               AS hours_observed
            FROM {weather}
            WHERE obs_date_local IS NOT NULL
            GROUP BY obs_date_local
        ),
        bounds AS (
            SELECT min(day) AS lo, max(day) AS hi FROM agg
        ),
        spine AS (
            SELECT unnest(generate_series(lo::TIMESTAMP, hi::TIMESTAMP, INTERVAL 1 DAY))::DATE
                AS day
            FROM bounds
        )
        SELECT
            spine.day                                  AS day,
            agg.air_temp_max_f, agg.air_temp_min_f, agg.air_temp_avg_f,
            agg.gdd50, agg.frost, agg.hard_freeze,
            agg.precip_total_in,
            agg.solar_rad_mean_wm2, agg.solar_rad_max_wm2,
            agg.surface_temp_max_f, agg.surface_temp_min_f,
            agg.rh_mean_pct,
            agg.soil_temp_5_f_mean, agg.soil_temp_10_f_mean, agg.soil_temp_20_f_mean,
            agg.soil_temp_50_f_mean, agg.soil_temp_100_f_mean,
            agg.soil_moisture_5_mean, agg.soil_moisture_10_mean, agg.soil_moisture_20_mean,
            agg.soil_moisture_50_mean, agg.soil_moisture_100_mean,
            COALESCE(agg.hours_observed, 0)            AS hours_observed,
            (agg.day IS NOT NULL)                      AS has_weather
        FROM spine
        LEFT JOIN agg ON agg.day = spine.day
    """
