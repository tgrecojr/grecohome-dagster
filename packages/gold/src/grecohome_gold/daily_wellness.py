"""The daily wellness mart — one row per local day, joining the four silver tables.

This is the analysis silver deliberately deferred: a continuous daily spine (so gaps
are explicit for rolling/streak analysis) left-joined to:

* **sleep** — ``silver_sleep`` (1:1 on ``night_date``), a curated subset of the
  Garmin + Whoop metrics (the full row stays in silver_sleep);
* **recovery** — ``silver_recovery`` deduped to one per day (latest ``created_at``;
  recovery's UTC ``recovery_date`` is occasionally 2-per-date);
* **workouts** — ``silver_workouts`` aggregated by ``activity_date`` (count + totals);
* **glucose** — ``silver_glucose`` aggregated by ``reading_date`` (mean / min / max /
  variability and **time-in-range**).

Per-day provenance flags (``has_sleep`` …) make every null explainable. Rebuildable:
the asset overwrites the mart from current silver each run.
"""

from __future__ import annotations

import os

# Time-in-range thresholds (mg/dL): non-diabetic / metabolic-health band.
TIR_LOW = 70
TIR_HIGH = 140


def _pq(silver_root: str, *parts: str) -> str:
    """A ``read_parquet('<silver_root>/.../x.parquet')`` source (single-quote escaped)."""
    path = os.path.join(silver_root, *parts)
    return f"read_parquet('{path.replace(chr(39), chr(39) * 2)}')"


def daily_wellness_sql(
    silver_root: str, *, tir_low: int = TIR_LOW, tir_high: int = TIR_HIGH
) -> str:
    """SQL for the daily wellness mart over the silver Parquet under ``silver_root``."""
    sleep = _pq(silver_root, "sleep", "silver_sleep.parquet")
    recovery = _pq(silver_root, "recovery", "silver_recovery.parquet")
    workouts = _pq(silver_root, "workouts", "silver_workouts.parquet")
    glucose = _pq(silver_root, "glucose", "silver_glucose.parquet")
    return f"""
        WITH bounds AS (
            SELECT least(
                (SELECT min(night_date) FROM {sleep}),
                (SELECT min(recovery_date) FROM {recovery}),
                (SELECT min(activity_date) FROM {workouts}),
                (SELECT min(reading_date) FROM {glucose})
            ) AS lo,
            greatest(
                (SELECT max(night_date) FROM {sleep}),
                (SELECT max(recovery_date) FROM {recovery}),
                (SELECT max(activity_date) FROM {workouts}),
                (SELECT max(reading_date) FROM {glucose})
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
        )
        SELECT
            spine.day                                          AS day,
            -- sleep (curated; full row in silver_sleep)
            s.garmin_sleep_score, s.garmin_total_min, s.garmin_rhr,
            s.whoop_performance_pct, s.whoop_efficiency_pct,
            -- recovery
            rec.recovery_score, rec.resting_heart_rate, rec.hrv_rmssd_milli, rec.spo2_percentage,
            -- workouts (daily aggregate)
            COALESCE(wo.workout_count, 0)                      AS workout_count,
            wo.workout_total_min, wo.workout_distance_km, wo.workout_calories,
            -- glucose (daily aggregate)
            glu.glucose_readings, glu.glucose_mean, glu.glucose_min, glu.glucose_max,
            glu.glucose_std, glu.glucose_tir_pct, glu.glucose_pct_below, glu.glucose_pct_above,
            -- provenance
            (s.night_date IS NOT NULL)   AS has_sleep,
            (rec.cycle_id IS NOT NULL)   AS has_recovery,
            (wo.day IS NOT NULL)         AS has_workout,
            (glu.day IS NOT NULL)        AS has_glucose
        FROM spine
        LEFT JOIN {sleep} AS s ON s.night_date = spine.day
        LEFT JOIN rec               ON rec.recovery_date = spine.day
        LEFT JOIN wo                ON wo.day = spine.day
        LEFT JOIN glu               ON glu.day = spine.day
    """
