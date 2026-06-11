"""Workout-specific silver column mapping (Garmin activities, single source).

A single-source reduction of the template: read the Garmin activities bronze, type +
deduplicate to one row per activity, write Parquet. One row per ``activityId``.

Source shape (profiled against live bronze): each bronze file is a top-level JSON
**array** of activity objects (the day/window's activities; empty ``[]`` on days with
none). Garmin captures append-only and re-fetch overlapping windows, so the same
activity recurs across many files — dedup by ``activityId`` is required. Activities
carry a **local** ``startTimeLocal`` and a UTC ``startTimeGMT`` (both space-ISO), so
the event date is just the local date — no timezone subtlety.

Notable fields: ``activityType.typeKey`` (running / cycling / yoga / …), ``duration``
/ ``movingDuration`` / ``elapsedDuration`` (seconds), ``distance`` (metres, null for
non-distance sports), ``calories``, ``averageHR`` / ``maxHR`` (``averageHR`` is 0 for
activities that don't record HR), and per-zone ``hrTimeInZone_1..5`` (present on a
subset). All extracted null-safe.
"""

from __future__ import annotations

from grecohome_core.silver import dedup_latest_sql, json_num, json_str, payloads_relation_sql

# Garmin filename carries the 13-digit fetch-millis; latest capture wins (an activity
# is immutable, but an edit/re-pull should take the newest).
_FETCHED_MS = r"TRY_CAST(regexp_extract(filename, '_([0-9]{13})_', 1) AS BIGINT)"


def _activities_relation(payloads_sql: str) -> str:
    """Unnest each file's top-level activity array to one row per activity (``a``)."""
    return (
        f"SELECT p.filename AS filename, a "
        f"FROM ({payloads_sql}) AS p, UNNEST(CAST(p.j AS JSON[])) AS t(a) "
        "WHERE json_type(p.j) = 'ARRAY'"
    )


def workouts_sql(files: list[str]) -> str:
    """Typed, deduped Garmin activities — one row per ``activityId``."""
    rel = _activities_relation(payloads_relation_sql(files))
    local = json_str("a", "startTimeLocal")
    typed = f"""
        SELECT
            TRY_CAST({json_str('a', 'activityId')} AS BIGINT)      AS activity_id,
            {json_str('a', 'activityName')}                        AS activity_name,
            {json_str('a', 'activityType.typeKey')}                AS activity_type,
            TRY_CAST(substr({local}, 1, 10) AS DATE)               AS activity_date,
            TRY_CAST({local} AS TIMESTAMP)                         AS start_time_local,
            TRY_CAST({json_str('a', 'startTimeGMT')} AS TIMESTAMP) AS start_time_gmt,
            {json_num('a', 'duration')}                            AS duration_sec,
            {json_num('a', 'movingDuration')}                      AS moving_duration_sec,
            {json_num('a', 'elapsedDuration')}                     AS elapsed_duration_sec,
            {json_num('a', 'distance')}                            AS distance_m,
            {json_num('a', 'calories')}                            AS calories,
            TRY_CAST({json_str('a', 'averageHR')} AS INTEGER)      AS avg_hr,
            TRY_CAST({json_str('a', 'maxHR')} AS INTEGER)          AS max_hr,
            {json_num('a', 'hrTimeInZone_1')}                      AS hr_z1_sec,
            {json_num('a', 'hrTimeInZone_2')}                      AS hr_z2_sec,
            {json_num('a', 'hrTimeInZone_3')}                      AS hr_z3_sec,
            {json_num('a', 'hrTimeInZone_4')}                      AS hr_z4_sec,
            {json_num('a', 'hrTimeInZone_5')}                      AS hr_z5_sec,
            TRY_CAST({json_str('a', 'deviceId')} AS BIGINT)        AS device_id,
            {_FETCHED_MS}                                          AS _fetched_ms
        FROM ({rel})
        WHERE {json_str('a', 'activityId')} IS NOT NULL
    """
    deduped = dedup_latest_sql(typed, partition_key="activity_id", order_by="_fetched_ms")
    return f"SELECT * EXCLUDE (_fetched_ms) FROM ({deduped})"


def bronze_activity_count_sql(files: list[str]) -> str:
    """Count of distinct bronze activities (distinct ``activityId``) — for coverage."""
    rel = _activities_relation(payloads_relation_sql(files))
    return (
        f"SELECT count(DISTINCT TRY_CAST({json_str('a', 'activityId')} AS BIGINT)) AS n "
        f"FROM ({rel})"
    )
