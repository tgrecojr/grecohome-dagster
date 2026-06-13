"""Fitness-snapshot silver column mapping (Garmin, multi-collection, single subject).

One typed row per **snapshot day** — Garmin's "current value, carried until it changes"
fitness state, joined across three Garmin collections by day:

* **VO2max** — ``garmin/max_metrics`` (``generic.vo2MaxValue`` running, ``cycling.vo2MaxValue``);
* **training status** — ``garmin/training_status`` (the device-keyed ``latestTrainingStatusData``
  map → ``trainingStatus`` code, ``weeklyTrainingLoad``, feedback phrase);
* **race predictions** — ``garmin/race_predictions`` (``time5K`` / ``10K`` / ``HalfMarathon`` /
  ``Marathon``, seconds).

**Date = the snapshot day (the bronze ``dt`` partition).** Unlike the event-based silver
tables, these are *current-state snapshot* endpoints — the payload carries no deep history
(``max_metrics`` has no date at all; ``race_predictions``' ``calendarDate`` only moves when the
prediction changes). So the meaningful day is **when the snapshot was taken** = the ``dt``
partition. Each collection is deduped to the **latest capture per ``dt``**, then the three are
spine-joined on the day so a day present in any collection yields a row (the others null).

**Coverage note:** the Garmin capture began 2026-06-03, and these endpoints don't backfill,
so history starts there and grows ~1 day/day — sparse today (VO2max only changes on run/ride
days), filling over time. Silver is rebuildable, so a later rebuild reprocesses all of bronze.
``endurance_score`` / ``hill_score`` are intentionally omitted: their values sit in nested
windowed DTO lists with the accessible top-level fields null — revisit if needed.
"""

from __future__ import annotations

from grecohome_core.silver import dedup_latest_sql, json_str, payloads_relation_sql

# The snapshot day = the bronze dt partition; recency tie-break = the fetch-millis. Both come
# from the bronze filename (``{collection}/dt=YYYY-MM-DD/{collection}_{fetched_ms}_{short}.ext``).
_DT = r"TRY_CAST(regexp_extract(filename, 'dt=([0-9]{4}-[0-9]{2}-[0-9]{2})', 1) AS DATE)"
_FETCHED_MS = r"TRY_CAST(regexp_extract(filename, '_([0-9]{13})_', 1) AS BIGINT)"

# The training-status device map: latestTrainingStatusData is keyed by device id; take the
# first device's entry (single-user, typically one primary device).
_TS_MAP = "j -> 'mostRecentTrainingStatus' -> 'latestTrainingStatusData'"
_TS_DEV = f"({_TS_MAP} -> (json_keys({_TS_MAP})[1]))"


def _dedup_day(typed_sql: str) -> str:
    """Keep the latest capture per ``snapshot_date`` (drops the recency helper)."""
    deduped = dedup_latest_sql(typed_sql, partition_key="snapshot_date", order_by="_fetched_ms")
    return f"SELECT * EXCLUDE (_fetched_ms) FROM ({deduped})"


def _max_metrics_rel(files: list[str]) -> str:
    """One row per snapshot day: running + cycling VO2max (from the first array element).

    Most ``max_metrics`` captures are empty arrays (the endpoint only returns VO2max on
    run/ride days), so we keep only **value-bearing** captures before dedup — otherwise a
    later empty re-capture of the same ``dt`` would null out a value an earlier capture had.
    """
    typed = f"""
        SELECT {_DT} AS snapshot_date,
            TRY_CAST(json_extract_string(j -> 0 -> 'generic', '$.vo2MaxValue') AS DOUBLE)
                AS vo2max_running,
            TRY_CAST(json_extract_string(j -> 0 -> 'cycling', '$.vo2MaxValue') AS DOUBLE)
                AS vo2max_cycling,
            {_FETCHED_MS} AS _fetched_ms
        FROM ({payloads_relation_sql(files)})
        WHERE {_DT} IS NOT NULL
    """
    valued = (
        f"SELECT * FROM ({typed}) "
        "WHERE vo2max_running IS NOT NULL OR vo2max_cycling IS NOT NULL"
    )
    return _dedup_day(valued)


def _training_status_rel(files: list[str]) -> str:
    """One row per snapshot day: training-status code, weekly load, feedback phrase."""
    typed = f"""
        SELECT {_DT} AS snapshot_date,
            TRY_CAST(json_extract_string({_TS_DEV}, '$.trainingStatus') AS INTEGER)
                AS training_status_code,
            TRY_CAST(json_extract_string({_TS_DEV}, '$.weeklyTrainingLoad') AS INTEGER)
                AS weekly_training_load,
            json_extract_string({_TS_DEV}, '$.trainingStatusFeedbackPhrase')
                AS training_status_phrase,
            {_FETCHED_MS} AS _fetched_ms
        FROM ({payloads_relation_sql(files)})
        WHERE {_DT} IS NOT NULL
    """
    return _dedup_day(typed)


def _race_predictions_rel(files: list[str]) -> str:
    """One row per snapshot day: 5K / 10K / half / marathon predicted times (seconds)."""
    typed = f"""
        SELECT {_DT} AS snapshot_date,
            TRY_CAST({json_str('j', 'time5K')} AS INTEGER)            AS race_5k_sec,
            TRY_CAST({json_str('j', 'time10K')} AS INTEGER)           AS race_10k_sec,
            TRY_CAST({json_str('j', 'timeHalfMarathon')} AS INTEGER)  AS race_half_marathon_sec,
            TRY_CAST({json_str('j', 'timeMarathon')} AS INTEGER)      AS race_marathon_sec,
            {_FETCHED_MS} AS _fetched_ms
        FROM ({payloads_relation_sql(files)})
        WHERE {_DT} IS NOT NULL
    """
    return _dedup_day(typed)


def fitness_sql(
    max_metrics_files: list[str],
    training_status_files: list[str],
    race_predictions_files: list[str],
) -> str:
    """One row per snapshot day, spine-joining the three Garmin fitness collections."""
    mm = _max_metrics_rel(max_metrics_files)
    ts = _training_status_rel(training_status_files)
    rp = _race_predictions_rel(race_predictions_files)
    return f"""
        WITH mm AS ({mm}), ts AS ({ts}), rp AS ({rp}),
        days AS (
            SELECT snapshot_date FROM mm
            UNION SELECT snapshot_date FROM ts
            UNION SELECT snapshot_date FROM rp
        )
        SELECT
            d.snapshot_date,
            mm.vo2max_running, mm.vo2max_cycling,
            ts.training_status_code, ts.weekly_training_load, ts.training_status_phrase,
            rp.race_5k_sec, rp.race_10k_sec, rp.race_half_marathon_sec, rp.race_marathon_sec
        FROM days d
        LEFT JOIN mm ON mm.snapshot_date = d.snapshot_date
        LEFT JOIN ts ON ts.snapshot_date = d.snapshot_date
        LEFT JOIN rp ON rp.snapshot_date = d.snapshot_date
        WHERE d.snapshot_date IS NOT NULL
    """


def bronze_snapshot_count_sql(
    max_metrics_files: list[str],
    training_status_files: list[str],
    race_predictions_files: list[str],
) -> str:
    """Count of distinct snapshot days across the three collections — for coverage."""
    return f"SELECT count(*) AS n FROM ({fitness_sql(max_metrics_files, training_status_files, race_predictions_files)})"  # noqa: E501
