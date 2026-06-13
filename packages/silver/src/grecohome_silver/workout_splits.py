"""Per-lap silver column mapping (Garmin activity splits, single source).

The per-lap breakdown ``silver_workouts`` doesn't have — one typed row per **lap**
(``activity_id`` + ``lap_index``), enriching each workout with its splits.

Source shape (profiled against live bronze: 727 activities, 4,613 laps): each
``garmin/activity_splits`` file is ``{activityId, lapDTOs: [...], eventDTOs}`` for one
activity; ``lapDTOs`` is the array of laps. ``activityId`` is in the payload (joins
``silver_workouts.activity_id``), so no sidecar is needed. Garmin re-pulls an activity into
several files, so dedup by **(activity_id, lap_index)** keeping the latest fetch.

Units kept canonical to the source: distance in metres, speed in m/s, durations in seconds,
HR in bpm. Lap richness varies by activity type (distance/speed/HR near-universal; calories
and elevation on a subset) — a missing field is NULL, the lap is kept.

HR *zones* are **not** here — they already live in ``silver_workouts`` (``hr_z1_sec`` …).
This table is specifically the lap/split grain.
"""

from __future__ import annotations

from grecohome_core.silver import dedup_latest_sql, json_num, json_str, payloads_relation_sql

# Bronze filename carries the 13-digit fetch-millis; latest re-pull wins the dedup.
_FETCHED_MS = r"TRY_CAST(regexp_extract(filename, '_([0-9]{13})_', 1) AS BIGINT)"


def _lap_relation(files: list[str]) -> str:
    """Unnest each file's ``lapDTOs`` to one row per lap.

    Carries ``filename`` (for the fetch-millis recency, which ``UNNEST`` would otherwise
    drop) and the payload ``activityId`` alongside each ``lap`` JSON value.
    """
    payloads_sql = payloads_relation_sql(files)
    return (
        f"SELECT p.filename AS filename, {json_str('p.j', 'activityId')} AS activity_id_str, "
        "lap "
        f"FROM ({payloads_sql}) AS p, "
        "UNNEST(CAST(p.j -> '$.lapDTOs' AS JSON[])) AS t(lap) "
        "WHERE json_type(p.j -> '$.lapDTOs') = 'ARRAY'"
    )


def splits_sql(files: list[str]) -> str:
    """Typed, deduped Garmin laps — one row per ``(activity_id, lap_index)`` (latest fetch)."""
    typed = f"""
        SELECT
            TRY_CAST(activity_id_str AS BIGINT)                        AS activity_id,
            TRY_CAST({json_str('lap', 'lapIndex')} AS INTEGER)         AS lap_index,
            TRY_CAST({json_str('lap', 'startTimeGMT')} AS TIMESTAMP)   AS lap_start_gmt,
            {json_num('lap', 'duration')}                              AS duration_sec,
            {json_num('lap', 'movingDuration')}                        AS moving_duration_sec,
            {json_num('lap', 'distance')}                              AS distance_m,
            {json_num('lap', 'averageSpeed')}                          AS avg_speed_mps,
            {json_num('lap', 'maxSpeed')}                              AS max_speed_mps,
            TRY_CAST({json_str('lap', 'averageHR')} AS INTEGER)        AS avg_hr,
            TRY_CAST({json_str('lap', 'maxHR')} AS INTEGER)            AS max_hr,
            {json_num('lap', 'calories')}                              AS calories,
            {json_num('lap', 'elevationGain')}                         AS elevation_gain_m,
            {json_num('lap', 'elevationLoss')}                         AS elevation_loss_m,
            {_FETCHED_MS}                                              AS _fetched_ms
        FROM ({_lap_relation(files)})
        WHERE activity_id_str IS NOT NULL AND {json_str('lap', 'lapIndex')} IS NOT NULL
    """
    deduped = dedup_latest_sql(
        typed, partition_key="activity_id, lap_index", order_by="_fetched_ms"
    )
    return f"SELECT * EXCLUDE (_fetched_ms) FROM ({deduped})"


def bronze_lap_count_sql(files: list[str]) -> str:
    """Count of distinct bronze laps (distinct ``activity_id`` + ``lapIndex``) — coverage."""
    return (
        "SELECT count(DISTINCT (activity_id_str || '-' || "
        f"{json_str('lap', 'lapIndex')})) AS n FROM ({_lap_relation(files)}) "
        f"WHERE activity_id_str IS NOT NULL AND {json_str('lap', 'lapIndex')} IS NOT NULL"
    )
