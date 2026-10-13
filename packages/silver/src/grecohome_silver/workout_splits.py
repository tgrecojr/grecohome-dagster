"""Per-lap silver column mapping (Garmin activity splits, single source).

The per-lap breakdown ``silver_workouts`` doesn't have — one typed row per **lap**
(``activity_id`` + ``lap_index``), enriching each workout with its splits.

Source shape (profiled against live bronze: 727 activities, 4,613 laps): each
``garmin/activity_splits`` file is ``{activityId, lapDTOs: [...], eventDTOs}`` for one
activity; ``lapDTOs`` is the array of laps. ``activityId`` is in the payload (joins
``silver_workouts.activity_id``), so no sidecar is needed. Garmin re-pulls an activity into
several files, so each activity's laps all come from its latest capture with ``lapDTOs``.

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
    """One row per lap, taken from each activity's latest capture that has ``lapDTOs``.

    Every payload whose ``lapDTOs`` is present (not missing, not JSON null) is cast to an
    array first, so a non-array ``lapDTOs`` in any capture fails the rebuild. Among those,
    each activity's newest capture (by the filename's fetch-millis; ties all kept) supplies
    all of its laps, so an activity re-lapped to fewer laps drops the old ones, and an empty
    ``lapDTOs`` leaves it with none. Carries ``filename`` and the payload ``activityId``
    alongside each ``lap`` JSON value.
    """
    lap_dtos = "p.j -> '$.lapDTOs'"
    with_laps = (
        f"SELECT p.filename AS filename, {json_str('p.j', 'activityId')} AS activity_id_str, "
        f"{_FETCHED_MS.replace('filename', 'p.filename')} AS fetched_ms, "
        f"CAST({lap_dtos} AS JSON[]) AS laps "
        f"FROM ({payloads_relation_sql(files)}) AS p "
        f"WHERE coalesce(json_type({lap_dtos}), 'NULL') <> 'NULL'"
    )
    latest = (
        "SELECT * FROM ("
        "  SELECT *, rank() OVER ("
        "    PARTITION BY activity_id_str ORDER BY fetched_ms DESC NULLS LAST"
        f"  ) AS _capture_rank FROM ({with_laps})"
        ") WHERE _capture_rank = 1"
    )
    return (
        "SELECT c.filename AS filename, c.activity_id_str AS activity_id_str, lap "
        f"FROM ({latest}) AS c, UNNEST(c.laps) AS t(lap)"
    )


# @spec SILVER-WORKOUTS-SPLITS-001, SILVER-WORKOUTS-SPLITS-002, SILVER-WORKOUTS-SPLITS-003
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
    """Distinct numeric (``activityId``, ``lapIndex``) pairs in the captures the table uses."""
    lap_index = f"TRY_CAST({json_str('lap', 'lapIndex')} AS INTEGER)"
    activity_id = "TRY_CAST(activity_id_str AS BIGINT)"
    return (
        f"SELECT count(DISTINCT ({activity_id}, {lap_index})) AS n "
        f"FROM ({_lap_relation(files)}) "
        f"WHERE {activity_id} IS NOT NULL AND {lap_index} IS NOT NULL"
    )
