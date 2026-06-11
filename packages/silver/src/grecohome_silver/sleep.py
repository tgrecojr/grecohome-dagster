"""Sleep-specific silver column mapping (the one place payload fields are named).

Two co-equal sources, preserved side by side — neither authoritative. Each silver
asset is built as a SQL string over a bronze-payloads subquery (see
:mod:`grecohome_core.silver`), so the assets stay thin: list files, build SQL,
write Parquet.

Source shapes (profiled against live bronze, spec §1/§5):

* **Garmin** — one flat JSON object per file: a top-level ``dailySleepDTO`` plus a
  sibling top-level ``restingHeartRate``. Event date is
  ``dailySleepDTO.calendarDate`` (authoritative; never the partition ``dt``).
  Stage durations are in **seconds**.
* **Whoop** — ``{"records": [...]}``; each record is one sleep with a UUID ``id``,
  ``updated_at`` (rescored), ``start``/``end`` timestamps (UTC), ``timezone_offset``,
  ``nap`` flag, ``cycle_id``, and a nested ``score``. Event date (the night) is the
  **local** date of ``start`` (``start + timezone_offset``; see
  :func:`_whoop_local_night`), so it matches the date the user slept and aligns with
  Garmin's local ``calendarDate``. Stage durations are in **millis**.

Unit decision: **all stage durations normalized to minutes** (Garmin ``/60``, Whoop
``/60000``) so ``garmin_*_min`` and ``whoop_*_min`` are directly comparable.
"""

from __future__ import annotations

from grecohome_core.silver import dedup_latest_sql, json_date, json_num, json_str

# ---------------------------------------------------------------------------
# Recency for dedup
# ---------------------------------------------------------------------------
# Bronze filenames are ``{collection}_{fetched_ms}_{short_id}.{ext}``; the 13-digit
# epoch-millis fetch time is the natural Garmin tie-break (latest re-pull wins)
# without opening sidecars. NULLs sort last in dedup_latest_sql, so a parseable
# fetched_ms always beats an unparseable filename.
_FETCHED_MS = r"TRY_CAST(regexp_extract(filename, '_([0-9]{13})_', 1) AS BIGINT)"


def _ts_epoch_ms_or_iso(value_sql: str) -> str:
    """Parse a value that is either epoch-millis (Garmin GMT) or an ISO timestamp.

    Tries epoch-millis first (``make_timestamp`` takes micros, so ``* 1000``); on a
    non-numeric value that TRY yields NULL and we fall back to a direct timestamp
    cast. Either way an unparseable value becomes NULL rather than erroring.
    """
    return (
        f"COALESCE("
        f"TRY_CAST(make_timestamp(TRY_CAST({value_sql} AS BIGINT) * 1000) AS TIMESTAMP), "
        f"TRY_CAST({value_sql} AS TIMESTAMP))"
    )


def _whoop_local_night(rec: str) -> str:
    """The local calendar night for a Whoop sleep record.

    Whoop ``start`` is a UTC timestamp; the user's bedtime is evening *local* time,
    which for a negative UTC offset is after midnight UTC — so ``CAST(start AS DATE)``
    in UTC dates ~93% of nights a day late (verified against live bronze) and
    misaligns with Garmin's local ``calendarDate``. We shift ``start`` by the
    record's own ``timezone_offset`` (``±HH:MM``) and take that date, so the night
    matches the date the user actually slept. Falls back to the UTC date if the
    offset is missing/unparseable (so a bad offset never drops the night).

    The offset's minutes inherit the hours' sign (``-04:30`` → −4h −30m).
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


# ---------------------------------------------------------------------------
# Garmin: bronze garmin/sleep -> one typed row per night
# ---------------------------------------------------------------------------
def garmin_sleep_sql(payloads_sql: str) -> str:
    """Typed, deduped Garmin sleep — one row per ``night_date`` (calendarDate).

    Garmin re-pulls the same night into many files; dedup by ``calendarDate`` keeping
    the latest fetch. Older nights lack an overall score — the night is kept, the
    score is null (never dropped).
    """
    dto = "dailySleepDTO."
    typed = f"""
        SELECT
            {json_date('j', dto + 'calendarDate')}                      AS night_date,
            TRY_CAST({json_str('j', dto + 'sleepScores.overall.value')} AS INTEGER)
                                                                        AS garmin_sleep_score,
            {json_num('j', dto + 'sleepTimeSeconds')}  / 60.0           AS garmin_total_min,
            {json_num('j', dto + 'deepSleepSeconds')}  / 60.0           AS garmin_deep_min,
            {json_num('j', dto + 'lightSleepSeconds')} / 60.0           AS garmin_light_min,
            {json_num('j', dto + 'remSleepSeconds')}   / 60.0           AS garmin_rem_min,
            {json_num('j', dto + 'awakeSleepSeconds')} / 60.0           AS garmin_awake_min,
            {json_num('j', dto + 'avgSleepStress')}                     AS garmin_avg_stress,
            {json_num('j', dto + 'averageRespirationValue')}            AS garmin_resp_avg,
            {json_num('j', dto + 'averageSpO2Value')}                   AS garmin_spo2_avg,
            TRY_CAST({json_str('j', 'restingHeartRate')} AS INTEGER)    AS garmin_rhr,
            {_ts_epoch_ms_or_iso(json_str('j', dto + 'sleepStartTimestampGMT'))}
                                                                        AS garmin_start_gmt,
            {_ts_epoch_ms_or_iso(json_str('j', dto + 'sleepEndTimestampGMT'))}
                                                                        AS garmin_end_gmt,
            {_FETCHED_MS}                                               AS _fetched_ms
        FROM ({payloads_sql})
        WHERE {json_date('j', dto + 'calendarDate')} IS NOT NULL
    """
    deduped = dedup_latest_sql(typed, partition_key="night_date", order_by="_fetched_ms")
    return f"SELECT * EXCLUDE (_fetched_ms) FROM ({deduped})"


# ---------------------------------------------------------------------------
# Whoop: bronze whoop/sleep -> one typed row per sleep id (naps flagged)
# ---------------------------------------------------------------------------
def whoop_sleep_sql(payloads_sql: str) -> str:
    """Typed, deduped Whoop sleep — one row per ``id``, naps flagged (``is_nap``).

    Whoop rescores, so dedup by ``id`` keeping the latest ``updated_at``. Naps are
    kept here (real data) and flagged; the unified night-grain join excludes them.
    """
    score = "score."
    stage = "score.stage_summary."
    records = (
        "SELECT p.filename AS filename, rec AS r "
        f"FROM ({payloads_sql}) AS p, "
        "UNNEST(CAST(p.j -> '$.records' AS JSON[])) AS t(rec)"
    )
    typed = f"""
        SELECT
            {_whoop_local_night('r')}                                   AS night_date,
            {json_str('r', 'id')}                                       AS whoop_sleep_id,
            COALESCE(TRY_CAST({json_str('r', 'nap')} AS BOOLEAN), FALSE) AS is_nap,
            {json_num('r', score + 'sleep_performance_percentage')}     AS whoop_performance_pct,
            {json_num('r', score + 'sleep_efficiency_percentage')}      AS whoop_efficiency_pct,
            {json_num('r', score + 'sleep_consistency_percentage')}     AS whoop_consistency_pct,
            {json_num('r', score + 'respiratory_rate')}                 AS whoop_resp_rate,
            {json_num('r', stage + 'total_slow_wave_sleep_time_milli')} / 60000.0
                                                                        AS whoop_deep_min,
            {json_num('r', stage + 'total_rem_sleep_time_milli')}   / 60000.0
                                                                        AS whoop_rem_min,
            {json_num('r', stage + 'total_light_sleep_time_milli')} / 60000.0
                                                                        AS whoop_light_min,
            {json_num('r', stage + 'total_awake_time_milli')}       / 60000.0
                                                                        AS whoop_awake_min,
            TRY_CAST({json_str('r', stage + 'disturbance_count')} AS INTEGER)
                                                                        AS whoop_disturbances,
            TRY_CAST({json_str('r', 'cycle_id')} AS BIGINT)             AS whoop_cycle_id,
            TRY_CAST({json_str('r', 'start')} AS TIMESTAMP)             AS whoop_start,
            TRY_CAST({json_str('r', 'end')} AS TIMESTAMP)               AS whoop_end,
            {json_str('r', 'updated_at')}                               AS _updated_at
        FROM ({records})
        WHERE {json_str('r', 'id')} IS NOT NULL
          AND {_whoop_local_night('r')} IS NOT NULL
    """
    deduped = dedup_latest_sql(typed, partition_key="whoop_sleep_id", order_by="_updated_at")
    return f"SELECT * EXCLUDE (_updated_at) FROM ({deduped})"


#: Whoop columns carried into the unified row (excludes id/is_nap, which are
#: source-asset bookkeeping, not per-night facts).
_WHOOP_UNIFIED_COLS = (
    "whoop_performance_pct",
    "whoop_efficiency_pct",
    "whoop_consistency_pct",
    "whoop_resp_rate",
    "whoop_deep_min",
    "whoop_rem_min",
    "whoop_light_min",
    "whoop_awake_min",
    "whoop_disturbances",
    "whoop_cycle_id",
    "whoop_start",
    "whoop_end",
)
_GARMIN_UNIFIED_COLS = (
    "garmin_sleep_score",
    "garmin_total_min",
    "garmin_deep_min",
    "garmin_light_min",
    "garmin_rem_min",
    "garmin_awake_min",
    "garmin_avg_stress",
    "garmin_resp_avg",
    "garmin_spo2_avg",
    "garmin_rhr",
    "garmin_start_gmt",
    "garmin_end_gmt",
)


# ---------------------------------------------------------------------------
# Unified: FULL OUTER JOIN on the night, both sides side by side
# ---------------------------------------------------------------------------
def unified_sleep_sql(garmin_sql: str, whoop_sql: str) -> str:
    """FULL OUTER JOIN of the two source assets on ``night_date`` — one row/night.

    Both sources' columns are kept side by side and nullable; nothing is coalesced
    and no "primary/best" value is synthesized. ``has_garmin``/``has_whoop`` make
    every null explainable. The Whoop side is collapsed to one non-nap record per
    night (latest ``updated_at``) so the join stays one-to-one per night.
    """
    # Whoop night spine: real nights only (no naps), one row per night.
    whoop_nights = dedup_latest_sql(
        f"SELECT * FROM ({whoop_sql}) WHERE is_nap = FALSE",
        partition_key="night_date",
        order_by="whoop_start",
    )
    g_cols = ",\n            ".join(f"g.{c} AS {c}" for c in _GARMIN_UNIFIED_COLS)
    w_cols = ",\n            ".join(f"w.{c} AS {c}" for c in _WHOOP_UNIFIED_COLS)
    return f"""
        SELECT
            COALESCE(g.night_date, w.night_date) AS night_date,
            {g_cols},
            {w_cols},
            (g.night_date IS NOT NULL) AS has_garmin,
            (w.night_date IS NOT NULL) AS has_whoop
        FROM ({garmin_sql}) AS g
        FULL OUTER JOIN ({whoop_nights}) AS w
            ON g.night_date = w.night_date
    """
