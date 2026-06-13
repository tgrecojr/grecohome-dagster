"""The daily wellness mart — one row per local day, joining the silver tables.

A continuous daily spine (so gaps are explicit for rolling/streak analysis) left-joined to:

* **sleep** — ``silver_sleep`` (1:1 on ``night_date``), a curated subset of the Garmin +
  Whoop metrics (the full row stays in silver_sleep);
* **recovery** — ``silver_recovery`` deduped to one per day (latest ``created_at``);
* **strain** — ``silver_strain`` joined to the recovery's **cycle** (``cycle_id``), so the
  day's exertion sits next to its recovery (kilojoules → kcal here);
* **daily activity** — ``silver_daily`` (Garmin ``user_summary``, 1:1 on the day): steps,
  active calories, distance, floors, intensity minutes, stress, body-battery;
* **workouts** — ``silver_workouts`` aggregated by ``activity_date`` (count + totals);
* **glucose** — ``silver_glucose`` aggregated by ``reading_date`` (mean / min / max /
  variability and **time-in-range**);
* **weight** — ``silver_body`` as-of-joined (latest weigh-in **carried forward**, since
  weigh-ins are sparse), presented in **lb** alongside BMI / body-fat.

Per-day provenance flags (``has_sleep`` …) make every null explainable. Rebuildable: the
asset overwrites the mart from current silver each run.
"""

from __future__ import annotations

import os

# Time-in-range thresholds (mg/dL): non-diabetic / metabolic-health band.
TIR_LOW = 70
TIR_HIGH = 140

# Presentation conversions (silver is canonical SI; gold adds the everyday units).
LB_PER_KG = 2.20462  # weight kg → lb
KJ_PER_KCAL = 4.184  # Whoop kilojoules → kilocalories


# Columns this mart reads from each silver table, typed to match silver. Used to
# build a typed empty relation when a table's Parquet does not exist yet (a fresh
# deploy, or a newly-added table not materialized), so the missing dimension degrades
# to NULLs via the LEFT JOINs / provenance flags instead of failing the whole build.
# Join keys must keep their exact silver type (dates -> DATE, cycle_id -> BIGINT);
# other columns only feed SELECT/aggregates. Keep in sync with the columns referenced
# below (they live in this file's SQL, so they change together).
_SLEEP_COLS = {
    "night_date": "DATE", "garmin_sleep_score": "INTEGER", "garmin_total_min": "DOUBLE",
    "garmin_rhr": "INTEGER", "whoop_performance_pct": "DOUBLE", "whoop_efficiency_pct": "DOUBLE",
}
_RECOVERY_COLS = {
    "recovery_date": "DATE", "created_at": "TIMESTAMP", "cycle_id": "BIGINT",
    "recovery_score": "DOUBLE", "resting_heart_rate": "DOUBLE", "hrv_rmssd_milli": "DOUBLE",
    "spo2_percentage": "DOUBLE",
}
_WORKOUTS_COLS = {
    "activity_date": "DATE", "duration_sec": "DOUBLE", "distance_m": "DOUBLE", "calories": "DOUBLE",
}
_GLUCOSE_COLS = {"reading_date": "DATE", "mgdl": "INTEGER"}
_STRAIN_COLS = {
    "strain_date": "DATE", "cycle_id": "BIGINT", "day_strain": "DOUBLE", "kilojoules": "DOUBLE",
    "avg_heart_rate": "INTEGER", "max_heart_rate": "INTEGER",
}
_DAILY_COLS = {
    "activity_date": "DATE", "total_steps": "INTEGER", "active_kilocalories": "DOUBLE",
    "total_distance_m": "DOUBLE", "floors_ascended": "DOUBLE", "moderate_intensity_min": "INTEGER",
    "vigorous_intensity_min": "INTEGER", "avg_stress_level": "INTEGER",
    "body_battery_high": "INTEGER", "body_battery_low": "INTEGER",
}
_BODY_COLS = {
    "measured_date": "DATE", "weight_kg": "DOUBLE", "bmi": "DOUBLE", "body_fat_pct": "DOUBLE",
}


def _src(silver_root: str, parts: tuple[str, ...], cols: dict[str, str]) -> str:
    """A ``read_parquet('<silver_root>/.../x.parquet')`` source, or a typed empty
    relation (``SELECT NULL::T AS c, ... WHERE false``) when that Parquet does not
    exist yet — so a not-yet-materialized silver table degrades to NULLs instead of
    erroring the whole mart. ``cols`` are the columns this mart reads, typed to silver.
    """
    path = os.path.join(silver_root, *parts)
    if os.path.exists(path):
        return f"read_parquet('{path.replace(chr(39), chr(39) * 2)}')"
    empty = ", ".join(f"NULL::{typ} AS {name}" for name, typ in cols.items())
    return f"(SELECT {empty} WHERE false)"


def daily_wellness_sql(
    silver_root: str, *, tir_low: int = TIR_LOW, tir_high: int = TIR_HIGH
) -> str:
    """SQL for the daily wellness mart over the silver Parquet under ``silver_root``."""
    sleep = _src(silver_root, ("sleep", "silver_sleep.parquet"), _SLEEP_COLS)
    recovery = _src(silver_root, ("recovery", "silver_recovery.parquet"), _RECOVERY_COLS)
    workouts = _src(silver_root, ("workouts", "silver_workouts.parquet"), _WORKOUTS_COLS)
    glucose = _src(silver_root, ("glucose", "silver_glucose.parquet"), _GLUCOSE_COLS)
    strain = _src(silver_root, ("strain", "silver_strain.parquet"), _STRAIN_COLS)
    daily = _src(silver_root, ("daily", "silver_daily.parquet"), _DAILY_COLS)
    body = _src(silver_root, ("body", "silver_body.parquet"), _BODY_COLS)
    return f"""
        WITH bounds AS (
            SELECT least(
                (SELECT min(night_date) FROM {sleep}),
                (SELECT min(recovery_date) FROM {recovery}),
                (SELECT min(activity_date) FROM {workouts}),
                (SELECT min(reading_date) FROM {glucose}),
                (SELECT min(activity_date) FROM {daily}),
                (SELECT min(strain_date) FROM {strain})
            ) AS lo,
            greatest(
                (SELECT max(night_date) FROM {sleep}),
                (SELECT max(recovery_date) FROM {recovery}),
                (SELECT max(activity_date) FROM {workouts}),
                (SELECT max(reading_date) FROM {glucose}),
                (SELECT max(activity_date) FROM {daily}),
                (SELECT max(strain_date) FROM {strain})
            ) AS hi
        ),
        spine AS (
            SELECT unnest(generate_series(lo::TIMESTAMP, hi::TIMESTAMP, INTERVAL 1 DAY))::DATE
                AS day
            FROM bounds
        ),
        rec AS (
            SELECT * EXCLUDE (_rn) FROM (
                SELECT *, row_number() OVER (PARTITION BY recovery_date ORDER BY created_at DESC)
                    AS _rn
                FROM {recovery}
            ) WHERE _rn = 1
        ),
        wo AS (
            SELECT activity_date AS day,
                count(*)                       AS workout_count,
                sum(duration_sec) / 60.0       AS workout_total_min,
                sum(distance_m) / 1000.0       AS workout_distance_km,
                sum(calories)                  AS workout_calories
            FROM {workouts} GROUP BY activity_date
        ),
        glu AS (
            SELECT reading_date AS day,
                count(*) AS glucose_readings,
                avg(mgdl) AS glucose_mean,
                min(mgdl) AS glucose_min,
                max(mgdl) AS glucose_max,
                stddev_samp(mgdl) AS glucose_std,
                100.0 * count(*) FILTER (WHERE mgdl BETWEEN {tir_low} AND {tir_high})
                    / count(*) AS glucose_tir_pct,
                100.0 * count(*) FILTER (WHERE mgdl < {tir_low}) / count(*) AS glucose_pct_below,
                100.0 * count(*) FILTER (WHERE mgdl > {tir_high}) / count(*) AS glucose_pct_above
            FROM {glucose} WHERE mgdl IS NOT NULL GROUP BY reading_date
        ),
        bod AS (
            SELECT measured_date, weight_kg, bmi, body_fat_pct FROM {body}
        )
        SELECT
            spine.day                                          AS day,
            -- sleep (curated; full row in silver_sleep)
            s.garmin_sleep_score, s.garmin_total_min, s.garmin_rhr,
            s.whoop_performance_pct, s.whoop_efficiency_pct,
            -- recovery
            rec.recovery_score, rec.resting_heart_rate, rec.hrv_rmssd_milli, rec.spo2_percentage,
            -- strain (joined to the recovery's cycle)
            st.day_strain,
            st.kilojoules / {KJ_PER_KCAL}                      AS strain_kilocalories,
            st.avg_heart_rate                                  AS strain_avg_hr,
            st.max_heart_rate                                  AS strain_max_hr,
            -- daily activity (Garmin user_summary)
            da.total_steps                                     AS steps,
            da.active_kilocalories                             AS active_calories,
            da.total_distance_m / 1000.0                       AS distance_km,
            da.floors_ascended                                 AS floors,
            (da.moderate_intensity_min + da.vigorous_intensity_min) AS intensity_minutes,
            da.avg_stress_level                                AS avg_stress,
            da.body_battery_high, da.body_battery_low,
            -- workouts (daily aggregate)
            COALESCE(wo.workout_count, 0)                      AS workout_count,
            wo.workout_total_min, wo.workout_distance_km, wo.workout_calories,
            -- glucose (daily aggregate)
            glu.glucose_readings, glu.glucose_mean, glu.glucose_min, glu.glucose_max,
            glu.glucose_std, glu.glucose_tir_pct, glu.glucose_pct_below, glu.glucose_pct_above,
            -- body (latest weigh-in carried forward; presented in lb)
            bod.weight_kg, bod.weight_kg * {LB_PER_KG} AS weight_lb,
            bod.bmi AS body_bmi, bod.body_fat_pct,
            -- provenance
            (s.night_date IS NOT NULL)     AS has_sleep,
            (rec.cycle_id IS NOT NULL)     AS has_recovery,
            (st.cycle_id IS NOT NULL)      AS has_strain,
            (da.activity_date IS NOT NULL) AS has_daily,
            (wo.day IS NOT NULL)           AS has_workout,
            (glu.day IS NOT NULL)          AS has_glucose,
            (bod.measured_date IS NOT NULL) AS has_weight
        FROM spine
        LEFT JOIN {sleep} AS s ON s.night_date = spine.day
        LEFT JOIN rec               ON rec.recovery_date = spine.day
        LEFT JOIN {strain} AS st    ON st.cycle_id = rec.cycle_id
        LEFT JOIN {daily} AS da     ON da.activity_date = spine.day
        LEFT JOIN wo                ON wo.day = spine.day
        LEFT JOIN glu               ON glu.day = spine.day
        ASOF LEFT JOIN bod          ON spine.day >= bod.measured_date
    """
