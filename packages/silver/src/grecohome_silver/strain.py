"""Strain-specific silver column mapping (Whoop physiological cycle, single source).

A single-source reduction of the template — the exertion twin of ``silver_recovery``:
read the Whoop **cycle** bronze, type + deduplicate to one row per cycle, write Parquet.

Source shape (profiled against live bronze, 2025-12-18→present, ~179 cycles): the same
``{"records": [...]}`` envelope as recovery/sleep; each record is one physiological cycle
with a numeric ``id`` (the cycle id), ``start``/``end`` (UTC), ``timezone_offset``,
``created_at``/``updated_at`` (Whoop rescores), ``score_state``, and a nested ``score``
(``strain`` 0–21, ``kilojoule``, ``average_heart_rate``, ``max_heart_rate``).

* **Identity / dedup:** key on ``cycle_id`` (the record ``id``), keep the latest
  ``updated_at`` (Whoop rescores a cycle as the day fills) — exactly the recovery idiom.
  The ``cycle_id`` is the join key into ``silver_recovery.cycle_id`` and
  ``silver_sleep.whoop_cycle_id``, so gold can put strain next to recovery for the day.
* **Day:** ``strain_date`` is the **local date of ``start``** (a Whoop cycle begins at
  wake, so its start-day is the day the strain accumulated). Derived by shifting the UTC
  ``start`` by the record's own ``timezone_offset`` (``±HH:MM``, minutes inheriting the
  hours' sign), mirroring the sleep wake-date idiom; falls back to the UTC date of
  ``start`` if the offset is missing. ``strain_date`` is informational (a day can hold two
  short cycles) — uniqueness is on ``cycle_id``.
* **Units:** kept canonical to the source — ``kilojoules`` as Whoop reports it (gold
  converts to kcal). Strain is the unitless 0–21 Whoop scale.
"""

from __future__ import annotations

from grecohome_core.silver import (
    dedup_latest_sql,
    json_date,
    json_num,
    json_str,
    payloads_relation_sql,
)


def _cycle_relation(payloads_sql: str) -> str:
    """Unnest each file's ``records`` array to one row per cycle (``r``)."""
    return (
        f"SELECT p.filename AS filename, rec AS r "
        f"FROM ({payloads_sql}) AS p, UNNEST(CAST(p.j -> '$.records' AS JSON[])) AS t(rec) "
        "WHERE json_type(p.j -> '$.records') = 'ARRAY'"
    )


def _local_start_date(rec: str) -> str:
    """The local calendar date of a cycle's ``start`` (the day the strain accrued).

    ``start`` is UTC; shift by the record's ``timezone_offset`` (``±HH:MM``, the minutes
    field inheriting the hours' sign — ``-04:30`` → −4h −30m) and take the date. Falls
    back to the UTC date of ``start`` if the offset is missing/unparseable.
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


def strain_sql(files: list[str]) -> str:
    """Typed, deduped Whoop strain — one row per ``cycle_id`` (latest rescore)."""
    rel = _cycle_relation(payloads_relation_sql(files))
    score = "score."
    typed = f"""
        SELECT
            TRY_CAST({json_str('r', 'id')} AS BIGINT)                  AS cycle_id,
            {_local_start_date('r')}                                   AS strain_date,
            TRY_CAST({json_str('r', 'start')} AS TIMESTAMP)            AS start_ts,
            TRY_CAST({json_str('r', 'end')} AS TIMESTAMP)              AS end_ts,
            {json_num('r', score + 'strain')}                          AS day_strain,
            {json_num('r', score + 'kilojoule')}                       AS kilojoules,
            TRY_CAST({json_str('r', score + 'average_heart_rate')} AS INTEGER)
                                                                       AS avg_heart_rate,
            TRY_CAST({json_str('r', score + 'max_heart_rate')} AS INTEGER)
                                                                       AS max_heart_rate,
            {json_str('r', 'score_state')}                             AS score_state,
            {json_str('r', 'updated_at')}                              AS _updated_at
        FROM ({rel})
        WHERE {json_str('r', 'id')} IS NOT NULL
    """
    deduped = dedup_latest_sql(typed, partition_key="cycle_id", order_by="_updated_at")
    return f"SELECT * EXCLUDE (_updated_at) FROM ({deduped})"


def bronze_strain_count_sql(files: list[str]) -> str:
    """Count of distinct bronze cycles (distinct ``cycle_id``) — for coverage."""
    rel = _cycle_relation(payloads_relation_sql(files))
    return f"SELECT count(DISTINCT TRY_CAST({json_str('r', 'id')} AS BIGINT)) AS n FROM ({rel})"
