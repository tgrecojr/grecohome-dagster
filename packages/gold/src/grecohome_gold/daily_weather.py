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
* **coverage** — ``hours_observed`` (hours with any measurement), a per-field
  ``*_hours`` valid-hour count, and a ``has_weather`` provenance flag.

**Completeness gate.** USCRN sensors drop out for hours at a time (a dead probe, a
logger fault, a missed transmission), and the dropouts cluster — in 2026 the soil
sensors went missing mostly in the local afternoon. A daily mean/sum/extreme over the
surviving hours is then biased, not merely imprecise. So each family of aggregates is
reported **only when its own field has at least ``min_valid_hours`` valid hours** in
the day (default 22 of 24, which is the completeness NCEI applies to its own daily01
product and which tolerates the 23-hour DST day); otherwise the aggregate is NULL and
the ``*_hours`` column says why. Consumers that want partial-day values can lower the
threshold (``GOLD_WEATHER_MIN_VALID_HOURS``) — the counts are always present.

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

# Default completeness gate: valid hours a field needs before its daily aggregate is
# reported. 22 of 24 matches NCEI's daily01 behaviour (observed: soil temp present at
# 22-23 valid hours, missing at ≤ 21) and tolerates the 23-hour spring-forward day.
MIN_VALID_HOURS = 22

# Silver measurement columns that make an hour "observed" (whole-row-null hours are
# NOAA's "no transmission" placeholders, not observations).
_OBSERVED_ANY = (
    "air_temp_c",
    "precip_mm",
    "solar_rad_wm2",
    "rh_pct",
    *(f"soil_temp_{d}" for d in DEPTHS),
    *(f"soil_moisture_{d}" for d in DEPTHS),
)

# Columns this mart reads from silver_weather, typed to match silver. Used to build a
# typed empty relation when silver_weather has not materialized yet, so the mart yields
# an empty result instead of failing the whole build. Keep in sync with the SQL below.
_WEATHER_COLS = {
    "obs_date_local": "DATE",
    "air_temp_max_c": "DOUBLE",
    "air_temp_min_c": "DOUBLE",
    "air_temp_c": "DOUBLE",
    "precip_mm": "DOUBLE",
    "solar_rad_wm2": "DOUBLE",
    "surface_temp_max_c": "DOUBLE",
    "surface_temp_min_c": "DOUBLE",
    "rh_pct": "DOUBLE",
    **{f"soil_temp_{d}": "DOUBLE" for d in DEPTHS},
    **{f"soil_moisture_{d}": "DOUBLE" for d in DEPTHS},
}

# (aggregate output column, gating valid-hour column). Every value column in the mart
# is gated on the ``*_hours`` count of the field it derives from.
_GATED: tuple[tuple[str, str], ...] = (
    ("air_temp_max_f", "air_temp_hours"),
    ("air_temp_min_f", "air_temp_hours"),
    ("air_temp_avg_f", "air_temp_hours"),
    ("gdd50", "air_temp_hours"),
    ("frost", "air_temp_hours"),
    ("hard_freeze", "air_temp_hours"),
    ("precip_total_in", "precip_hours"),
    ("solar_rad_mean_wm2", "solar_hours"),
    ("solar_rad_max_wm2", "solar_hours"),
    ("surface_temp_max_f", "surface_temp_hours"),
    ("surface_temp_min_f", "surface_temp_hours"),
    ("rh_mean_pct", "rh_hours"),
    *((f"soil_temp_{d}_f_mean", f"soil_temp_{d}_hours") for d in DEPTHS),
    *((f"soil_moisture_{d}_mean", f"soil_moisture_{d}_hours") for d in DEPTHS),
)

# Valid-hour count columns, in output order.
HOUR_COLS = (
    "air_temp_hours",
    "precip_hours",
    "solar_hours",
    "surface_temp_hours",
    "rh_hours",
    *(f"soil_temp_{d}_hours" for d in DEPTHS),
    *(f"soil_moisture_{d}_hours" for d in DEPTHS),
)


def _src(silver_root: str, parts: tuple[str, ...], cols: dict[str, str]) -> str:
    """A ``read_parquet('<silver_root>/.../x.parquet')`` source, or a typed empty
    relation (``SELECT NULL::T AS c, ... WHERE false``) when that Parquet does not
    exist yet — so a not-yet-materialized silver_weather yields an empty mart instead
    of erroring. ``cols`` are the columns this mart reads, typed to silver.
    """
    path = os.path.join(silver_root, *parts)
    if os.path.exists(path):
        return f"read_parquet('{path.replace(chr(39), chr(39) * 2)}')"
    empty = ", ".join(f"NULL::{typ} AS {name}" for name, typ in cols.items())
    return f"(SELECT {empty} WHERE false)"


def _f(celsius_expr: str) -> str:
    """Celsius expression → Fahrenheit."""
    return f"({celsius_expr}) * 9.0 / 5.0 + 32.0"


def _agg_sql(weather: str, gdd_base_f: int, frost_f: int, hard_freeze_f: int) -> str:
    """Raw per-local-day aggregates + valid-hour counts (no gating yet)."""
    tmax_f = _f("max(air_temp_max_c)")
    tmin_f = _f("min(air_temp_min_c)")
    soil = ",\n            ".join(
        [
            *(f"{_f(f'avg(soil_temp_{d})')} AS soil_temp_{d}_f_mean" for d in DEPTHS),
            *(f"avg(soil_moisture_{d}) AS soil_moisture_{d}_mean" for d in DEPTHS),
            *(f"count(soil_temp_{d}) AS soil_temp_{d}_hours" for d in DEPTHS),
            *(f"count(soil_moisture_{d}) AS soil_moisture_{d}_hours" for d in DEPTHS),
        ]
    )
    observed = " OR ".join(f"{c} IS NOT NULL" for c in _OBSERVED_ANY)
    return f"""
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
            {soil},
            count(air_temp_c)                      AS air_temp_hours,
            count(precip_mm)                       AS precip_hours,
            count(solar_rad_wm2)                   AS solar_hours,
            count(surface_temp_max_c)              AS surface_temp_hours,
            count(rh_pct)                          AS rh_hours,
            count(*) FILTER (WHERE {observed})     AS hours_observed
        FROM {weather}
        WHERE obs_date_local IS NOT NULL
        GROUP BY obs_date_local
    """


def daily_weather_sql(
    silver_root: str,
    *,
    min_valid_hours: int = MIN_VALID_HOURS,
    gdd_base_f: int = GDD_BASE_F,
    frost_f: int = FROST_F,
    hard_freeze_f: int = HARD_FREEZE_F,
) -> str:
    """SQL for the daily weather mart over ``silver_weather`` under ``silver_root``.

    ``min_valid_hours`` is the completeness gate (see module docstring): an
    aggregate is NULL unless its field has at least that many valid hours that day.
    """
    weather = _src(silver_root, ("weather", "silver_weather.parquet"), _WEATHER_COLS)
    gated = ",\n            ".join(
        f"CASE WHEN agg.{hours} >= {int(min_valid_hours)} THEN agg.{col} END AS {col}"
        for col, hours in _GATED
    )
    hours = ",\n            ".join(f"COALESCE(agg.{c}, 0) AS {c}" for c in HOUR_COLS)
    return f"""
        WITH agg AS ({_agg_sql(weather, gdd_base_f, frost_f, hard_freeze_f)}),
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
            {gated},
            COALESCE(agg.hours_observed, 0)            AS hours_observed,
            {hours},
            (COALESCE(agg.hours_observed, 0) > 0)      AS has_weather
        FROM spine
        LEFT JOIN agg ON agg.day = spine.day
    """
