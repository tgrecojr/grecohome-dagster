"""Recovery-specific silver column mapping (Whoop recovery, single source).

A single-source reduction of the template: read the Whoop recovery bronze, type +
deduplicate to one row per cycle, write Parquet. One row per ``cycle_id``.

Source shape (profiled against live bronze): ``{"records": [...]}``; each record is a
recovery for one cycle/sleep — ``cycle_id`` and ``sleep_id`` (both 1:1 with the
recovery), ``created_at`` / ``updated_at`` (UTC; Whoop rescores), and a nested
``score`` (``recovery_score`` %, ``resting_heart_rate``, ``hrv_rmssd_milli``,
``spo2_percentage``, ``skin_temp_celsius``, ``user_calibrating``). Dedup by
``cycle_id`` keeping the latest ``updated_at``.

Recovery records carry **no timezone_offset**, so ``recovery_date`` is the UTC date of
``created_at`` (≈ the wake morning). For precise alignment, gold joins recovery to
``silver_sleep_whoop`` on ``sleep_id`` (= ``whoop_sleep_id``) or ``cycle_id``
(= ``whoop_cycle_id``), which carry the sleep's true local night.
"""

from __future__ import annotations

from grecohome_core.silver import (
    dedup_latest_sql,
    json_date,
    json_num,
    json_str,
    payloads_relation_sql,
)


def _recovery_relation(payloads_sql: str) -> str:
    """Unnest each file's ``records`` array to one row per recovery (``r``)."""
    return (
        f"SELECT p.filename AS filename, rec AS r "
        f"FROM ({payloads_sql}) AS p, UNNEST(CAST(p.j -> '$.records' AS JSON[])) AS t(rec) "
        "WHERE json_type(p.j -> '$.records') = 'ARRAY'"
    )


def recovery_sql(files: list[str]) -> str:
    """Typed, deduped Whoop recovery — one row per ``cycle_id`` (latest rescore)."""
    rel = _recovery_relation(payloads_relation_sql(files))
    score = "score."
    typed = f"""
        SELECT
            TRY_CAST({json_str('r', 'cycle_id')} AS BIGINT)            AS cycle_id,
            {json_str('r', 'sleep_id')}                                AS sleep_id,
            {json_date('r', 'created_at')}                             AS recovery_date,
            TRY_CAST({json_str('r', 'created_at')} AS TIMESTAMP)       AS created_at,
            {json_num('r', score + 'recovery_score')}                  AS recovery_score,
            {json_num('r', score + 'resting_heart_rate')}              AS resting_heart_rate,
            {json_num('r', score + 'hrv_rmssd_milli')}                 AS hrv_rmssd_milli,
            {json_num('r', score + 'spo2_percentage')}                 AS spo2_percentage,
            {json_num('r', score + 'skin_temp_celsius')}               AS skin_temp_celsius,
            COALESCE(TRY_CAST({json_str('r', score + 'user_calibrating')} AS BOOLEAN), FALSE)
                                                                       AS user_calibrating,
            {json_str('r', 'updated_at')}                              AS _updated_at
        FROM ({rel})
        WHERE {json_str('r', 'cycle_id')} IS NOT NULL
    """
    deduped = dedup_latest_sql(typed, partition_key="cycle_id", order_by="_updated_at")
    return f"SELECT * EXCLUDE (_updated_at) FROM ({deduped})"


def bronze_recovery_count_sql(files: list[str]) -> str:
    """Count of distinct bronze recoveries (distinct ``cycle_id``) — for coverage."""
    rel = _recovery_relation(payloads_relation_sql(files))
    return (
        f"SELECT count(DISTINCT TRY_CAST({json_str('r', 'cycle_id')} AS BIGINT)) AS n "
        f"FROM ({rel})"
    )
