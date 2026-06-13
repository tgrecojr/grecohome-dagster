"""Daily-summary silver column mapping (Garmin ``user_summary``, single source).

A single-source reduction of the template — one typed row per **local day**, the daily
movement-and-wellness rollup. ``garmin/user_summary`` is a flat daily super-object: a
single record already carries steps, distance, every calorie type, floors, intensity
minutes, resting/min/max HR, the full stress breakdown, body-battery, SpO2, and
respiration. So this one table subsumes the standalone per-metric daily collections
(``daily_steps``, ``floors``, ``intensity_minutes``, ``resting_heart_rate``,
``stress``/``spo2``/``respiration``/``body_battery`` daily values).

Source shape (profiled against live bronze, 2022-06→present, ~1331 days): one flat JSON
object per file with a ``calendarDate`` (authoritative, already local — the **event
date**). Garmin re-pulls a day into several files, so dedup by ``calendarDate`` keeping
the latest fetch (the 13-digit ``fetched_ms`` in the bronze filename), exactly the Garmin
sleep idiom. Coverage varies by day (older days predate some sensors); a missing field is
NULL, the day is kept.

Units kept canonical to the source: distance in metres, calories in kilocalories,
durations in seconds, intensity in minutes. Garmin's ``-1``/``-2`` "no-data" sentinels on
the stress *levels* become NULL.
"""

from __future__ import annotations

from grecohome_core.silver import (
    dedup_latest_sql,
    json_date,
    json_num,
    json_str,
    payloads_relation_sql,
)

# Bronze filename carries the 13-digit fetch-millis; latest re-pull wins the dedup.
_FETCHED_MS = r"TRY_CAST(regexp_extract(filename, '_([0-9]{13})_', 1) AS BIGINT)"

# (output column, source key) for integer-typed fields (counts / seconds / bpm).
_INT_FIELDS: list[tuple[str, str]] = [
    ("total_steps", "totalSteps"),
    ("step_goal", "dailyStepGoal"),
    ("moderate_intensity_min", "moderateIntensityMinutes"),
    ("vigorous_intensity_min", "vigorousIntensityMinutes"),
    ("intensity_minutes_goal", "intensityMinutesGoal"),
    ("min_heart_rate", "minHeartRate"),
    ("max_heart_rate", "maxHeartRate"),
    ("resting_heart_rate", "restingHeartRate"),
    ("body_battery_high", "bodyBatteryHighestValue"),
    ("body_battery_low", "bodyBatteryLowestValue"),
    ("body_battery_charged", "bodyBatteryChargedValue"),
    ("body_battery_drained", "bodyBatteryDrainedValue"),
    ("avg_spo2", "averageSpo2"),
    ("lowest_spo2", "lowestSpo2"),
    ("rest_stress_duration", "restStressDuration"),
    ("low_stress_duration", "lowStressDuration"),
    ("medium_stress_duration", "mediumStressDuration"),
    ("high_stress_duration", "highStressDuration"),
    ("highly_active_seconds", "highlyActiveSeconds"),
    ("active_seconds", "activeSeconds"),
    ("sedentary_seconds", "sedentarySeconds"),
    ("sleeping_seconds", "sleepingSeconds"),
]
# (output column, source key) for double-typed fields (metres / kcal / rates).
_DOUBLE_FIELDS: list[tuple[str, str]] = [
    ("total_distance_m", "totalDistanceMeters"),
    ("total_kilocalories", "totalKilocalories"),
    ("active_kilocalories", "activeKilocalories"),
    ("bmr_kilocalories", "bmrKilocalories"),
    ("floors_ascended", "floorsAscended"),
    ("floors_descended", "floorsDescended"),
    ("avg7d_resting_heart_rate", "lastSevenDaysAvgRestingHeartRate"),
    ("avg_waking_respiration", "avgWakingRespirationValue"),
    ("highest_respiration", "highestRespirationValue"),
    ("lowest_respiration", "lowestRespirationValue"),
]
# Stress levels (0–100) carry Garmin's -1/-2 "no data" sentinels → NULL.
_STRESS_FIELDS: list[tuple[str, str]] = [
    ("avg_stress_level", "averageStressLevel"),
    ("max_stress_level", "maxStressLevel"),
]


def _int(path: str) -> str:
    return f"TRY_CAST({json_str('j', path)} AS INTEGER)"


def daily_sql(files: list[str]) -> str:
    """Typed, deduped Garmin daily summary — one row per ``activity_date`` (latest fetch)."""
    cols = [f"{_int(p)} AS {a}" for a, p in _INT_FIELDS]
    cols += [f"{json_num('j', p)} AS {a}" for a, p in _DOUBLE_FIELDS]
    cols += [f"nullif(nullif({_int(p)}, -1), -2) AS {a}" for a, p in _STRESS_FIELDS]
    body = ",\n            ".join(cols)
    typed = f"""
        SELECT
            {json_date('j', 'calendarDate')}                AS activity_date,
            {body},
            {_FETCHED_MS}                                   AS _fetched_ms
        FROM ({payloads_relation_sql(files)})
        WHERE {json_date('j', 'calendarDate')} IS NOT NULL
    """
    deduped = dedup_latest_sql(typed, partition_key="activity_date", order_by="_fetched_ms")
    return f"SELECT * EXCLUDE (_fetched_ms) FROM ({deduped})"


def bronze_day_count_sql(files: list[str]) -> str:
    """Count of distinct bronze days (distinct ``calendarDate``) — for coverage."""
    return (
        f"SELECT count(DISTINCT {json_date('j', 'calendarDate')}) AS n "
        f"FROM ({payloads_relation_sql(files)})"
    )
