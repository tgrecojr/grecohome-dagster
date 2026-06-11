"""Glucose-specific silver column mapping (Lingo CGM, single source).

A single-source reduction of the sleep template: read the Lingo CGM CSV exports,
type + deduplicate to one row per reading, write Parquet. No join (one source).

Source shape (profiled against live bronze): a 2-column CSV (plus the bronze ``dt``
partition column) — a reading timestamp and the value in mg/dL. Cumulative exports
are re-uploaded constantly, so the same reading appears in many files (~13× raw
duplication).

Two profiling facts drive the design:

* **The reading time is already local, with its offset embedded** —
  ``2026-06-11T18:05-04:00`` — so the event date is just its local date; no UTC/
  wake-date subtlety like Whoop. We split it into a naive local wall-clock, the
  local date, the offset (minutes), and a derived UTC instant.
* **The UTC instant is the reading's true identity, not the local string.** The
  same physical reading is re-exported under different offset spellings (up to 4×,
  e.g. after a timezone change), so distinct local strings (~163k) far exceed
  distinct instants (~55k) — and every duplicate carries an *identical* value (zero
  conflicts). So we **dedup on the UTC instant** (lossless); keying on the local
  string would multiply readings ~3×. The UTC instant is computed arithmetically
  (local − offset), not by parsing the offset string (which lacks seconds and won't
  cast to ``TIMESTAMPTZ``).

Local-representation note: for ~11% of instants the *local date* differs across an
instant's offset spellings. The instant is canonical; we keep the **latest-captured**
export's local representation (``fetched_ms``), consistent with silver being a
projection of current bronze. ``mgdl`` is never ambiguous.
"""

from __future__ import annotations

from grecohome_core.silver import csv_relation_sql, dedup_latest_sql

# Exact Lingo CSV headers (mapped to safe aliases by the CSV reader).
_TS_COL = "Time of Glucose Reading [T=(local time) +/- (time zone offset)]"
_MGDL_COL = "Measurement(mg/dL)"

# Bronze filename carries the 13-digit fetch-millis; latest capture wins the
# representation tie-break for an instant (NULLs sort last in dedup_latest_sql).
_FETCHED_MS = r"TRY_CAST(regexp_extract(filename, '_([0-9]{13})_', 1) AS BIGINT)"


def _offset_minutes(ts: str) -> str:
    """Signed UTC offset, in minutes, parsed from a ``...±HH:MM`` local timestamp.

    The offset begins at char 17 (after ``YYYY-MM-DDTHH:MM``); the minutes field
    (chars 21–22) inherits the hours' sign (``-04:30`` → −4h −30m).
    """
    return (
        f"(TRY_CAST(substr({ts}, 17, 3) AS INTEGER) * 60 "
        f"+ (CASE WHEN substr({ts}, 17, 1) = '-' THEN -1 ELSE 1 END) "
        f"* COALESCE(TRY_CAST(substr({ts}, 21, 2) AS INTEGER), 0))"
    )


def _local_ts(ts: str) -> str:
    """The naive local wall-clock (``YYYY-MM-DDTHH:MM``) as a TIMESTAMP."""
    return f"TRY_CAST(substr({ts}, 1, 16) AS TIMESTAMP)"


def _utc_ts(ts: str) -> str:
    """The UTC instant, derived as local − offset (avoids parsing the offset string)."""
    return f"({_local_ts(ts)} - {_offset_minutes(ts)} * INTERVAL 1 MINUTE)"


def glucose_sql(files: list[str]) -> str:
    """Typed, deduped Lingo glucose — one row per reading (per UTC instant)."""
    raw = csv_relation_sql(files, {"ts_str": _TS_COL, "mgdl_str": _MGDL_COL})
    typed = f"""
        SELECT
            {_utc_ts('ts_str')}                                     AS reading_ts_utc,
            {_local_ts('ts_str')}                                   AS reading_ts_local,
            TRY_CAST(substr(ts_str, 1, 10) AS DATE)                 AS reading_date,
            {_offset_minutes('ts_str')}                             AS tz_offset_minutes,
            TRY_CAST(mgdl_str AS INTEGER)                           AS mgdl,
            {_FETCHED_MS}                                           AS _fetched_ms
        FROM ({raw})
        WHERE {_local_ts('ts_str')} IS NOT NULL
    """
    deduped = dedup_latest_sql(typed, partition_key="reading_ts_utc", order_by="_fetched_ms")
    return f"SELECT * EXCLUDE (_fetched_ms) FROM ({deduped})"


def bronze_reading_count_sql(files: list[str]) -> str:
    """Count of distinct bronze readings (distinct UTC instants) — for coverage checks."""
    raw = csv_relation_sql(files, {"ts_str": _TS_COL})
    return (
        f"SELECT count(DISTINCT {_utc_ts('ts_str')}) AS n "
        f"FROM ({raw}) WHERE {_local_ts('ts_str')} IS NOT NULL"
    )
