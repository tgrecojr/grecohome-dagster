"""Whoop-workout silver column mapping (Whoop activities, single source).

One typed row per Whoop **workout** — the activity story Garmin doesn't have: since the
device arrived (2025-12-18) Whoop logs strength, yard-work, walking, etc. (and running/
cycling that overlaps Garmin). Kept as its **own** table parallel to ``silver_workouts``
(Garmin) — same philosophy as sleep: two sources, neither authoritative, never blended.

Source shape (profiled against live bronze, 183 workouts): the same ``{"records": [...]}``
envelope as recovery/strain; each record has a UUID ``id``, ``start``/``end`` (UTC),
``timezone_offset``, ``created_at``/``updated_at`` (Whoop rescores), ``sport_name`` /
``sport_id``, ``score_state``, and a nested ``score`` (``strain`` 0–21,
``average_heart_rate``, ``max_heart_rate``, ``kilojoule``, ``percent_recorded``,
``distance_meter``, ``altitude_gain_meter``). Dedup by ``id`` keeping the latest
``updated_at``.

``workout_date`` is the **local date of ``start``** (the day the workout happened), derived
via the record's ``timezone_offset`` (``±HH:MM``), mirroring the sleep/strain idiom. Units
stay canonical to the source: ``kilojoules`` as Whoop reports it (gold/dashboard → kcal),
distance in metres; strain is the unitless 0–21 scale. Most non-GPS sports (lifting, yard-
work) have null distance — kept, never dropped.
"""

from __future__ import annotations

from grecohome_core.silver import (
    dedup_latest_sql,
    json_date,
    json_num,
    json_str,
    payloads_relation_sql,
)


def _workout_relation(payloads_sql: str) -> str:
    """Unnest each file's ``records`` array to one row per workout (``r``)."""
    return (
        f"SELECT p.filename AS filename, rec AS r "
        f"FROM ({payloads_sql}) AS p, UNNEST(CAST(p.j -> '$.records' AS JSON[])) AS t(rec) "
        "WHERE json_type(p.j -> '$.records') = 'ARRAY'"
    )


def _local_start_date(rec: str) -> str:
    """The local calendar date of a workout's ``start`` (the day it happened).

    ``start`` is UTC; shift by the record's ``timezone_offset`` (``±HH:MM``, the minutes
    field inheriting the hours' sign) and take the date. Falls back to the UTC date of
    ``start`` if the offset is missing/unparseable.
    """
    start_ts = f"TRY_CAST({json_str(rec, 'start')} AS TIMESTAMP)"
    tz = json_str(rec, "timezone_offset")
    offset_min = (
        f"(TRY_CAST(substr({tz}, 1, 3) AS INTEGER) * 60 "
        f"+ (CASE WHEN substr({tz}, 1, 1) = '-' THEN -1 ELSE 1 END) "
        f"* COALESCE(TRY_CAST(substr({tz}, 5, 2) AS INTEGER), 0))"
    )
    local_date = f"TRY_CAST(({start_ts} + {offset_min} * INTERVAL 1 MINUTE) AS DATE)"
    return f"COALESCE({local_date}, {json_date(rec, 'start')})"


def whoop_workouts_sql(files: list[str]) -> str:
    """Typed, deduped Whoop workouts — one row per ``workout_id`` (latest rescore)."""
    rel = _workout_relation(payloads_relation_sql(files))
    score = "score."
    typed = f"""
        SELECT
            {json_str('r', 'id')}                                      AS workout_id,
            {_local_start_date('r')}                                   AS workout_date,
            {json_str('r', 'sport_name')}                              AS sport_name,
            TRY_CAST({json_str('r', 'sport_id')} AS INTEGER)           AS sport_id,
            TRY_CAST({json_str('r', 'start')} AS TIMESTAMP)            AS start_ts,
            TRY_CAST({json_str('r', 'end')} AS TIMESTAMP)              AS end_ts,
            {json_num('r', score + 'strain')}                          AS strain,
            TRY_CAST({json_str('r', score + 'average_heart_rate')} AS INTEGER)
                                                                       AS avg_heart_rate,
            TRY_CAST({json_str('r', score + 'max_heart_rate')} AS INTEGER)
                                                                       AS max_heart_rate,
            {json_num('r', score + 'kilojoule')}                       AS kilojoules,
            {json_num('r', score + 'distance_meter')}                  AS distance_m,
            {json_num('r', score + 'altitude_gain_meter')}             AS altitude_gain_m,
            {json_num('r', score + 'percent_recorded')}                AS percent_recorded,
            {json_str('r', 'score_state')}                             AS score_state,
            {json_str('r', 'updated_at')}                              AS _updated_at
        FROM ({rel})
        WHERE {json_str('r', 'id')} IS NOT NULL
    """
    deduped = dedup_latest_sql(typed, partition_key="workout_id", order_by="_updated_at")
    return f"SELECT * EXCLUDE (_updated_at) FROM ({deduped})"


def bronze_workout_count_sql(files: list[str]) -> str:
    """Count of distinct bronze workouts (distinct ``id``) — for coverage."""
    rel = _workout_relation(payloads_relation_sql(files))
    return f"SELECT count(DISTINCT {json_str('r', 'id')}) AS n FROM ({rel})"
