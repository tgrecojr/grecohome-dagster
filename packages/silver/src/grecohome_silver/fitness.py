"""Fitness silver column mapping (Garmin, multi-collection, single subject).

One typed row per **day the values apply to** (``snapshot_date``), joining three Garmin
fitness collections on that day:

* **VO2max** (``garmin/max_metrics``): the running (``generic``) and cycling values, **each
  dated by its own** ``calendarDate``. Most captures are empty arrays (VO2max is only reported
  on run/ride days), so only captures carrying a value take part; the latest per sport and date
  wins.
* **Training status** (``garmin/training_status``): ``latestTrainingStatusData`` holds one entry
  per device, each dated by its own ``calendarDate`` (the day that status took effect, so values
  repeated in later captures collapse onto that day). Per date, the entry Garmin flags
  ``primaryTrainingDevice`` wins, then the latest capture: for this user the Edge 1040 bike
  computer on ride days, the Forerunner 945 otherwise. ``training_device_id`` records which.
* **Race predictions** (``garmin/race_predictions``): no date that tracks the snapshot, so dated
  by the bronze ``dt`` folder (the once-daily capture's UTC day); latest capture per date.

A day present in any of the three yields a row; the others' columns are null.
"""

from __future__ import annotations

from grecohome_core.silver import dedup_latest_sql, json_str, payloads_relation_sql

# The snapshot day = the bronze dt partition; recency tie-break = the fetch-millis. Both come
# from the bronze filename (``{collection}/dt=YYYY-MM-DD/{collection}_{fetched_ms}_{short}.ext``).
_DT = r"TRY_CAST(regexp_extract(filename, 'dt=([0-9]{4}-[0-9]{2}-[0-9]{2})', 1) AS DATE)"
_FETCHED_MS = r"TRY_CAST(regexp_extract(filename, '_([0-9]{13})_', 1) AS BIGINT)"

# The training-status map: latestTrainingStatusData holds one entry per device id.
_TS_MAP = "j -> 'mostRecentTrainingStatus' -> 'latestTrainingStatusData'"


def _dedup_day(typed_sql: str) -> str:
    """Keep the latest capture per ``snapshot_date`` (drops the recency helper)."""
    deduped = dedup_latest_sql(typed_sql, partition_key="snapshot_date", order_by="_fetched_ms")
    return f"SELECT * EXCLUDE (_fetched_ms) FROM ({deduped})"


def _vo2max_rel(files: list[str], sport: str, column: str) -> str:
    """One row per date for one sport's VO2max, dated by that sport's ``calendarDate``."""
    node = f"j -> 0 -> '{sport}'"
    typed = f"""
        SELECT TRY_CAST(json_extract_string({node}, '$.calendarDate') AS DATE) AS snapshot_date,
            TRY_CAST(json_extract_string({node}, '$.vo2MaxValue') AS DOUBLE) AS {column},
            {_FETCHED_MS} AS _fetched_ms
        FROM ({payloads_relation_sql(files)})
    """
    valued = (
        f"SELECT * FROM ({typed}) WHERE {column} IS NOT NULL AND snapshot_date IS NOT NULL"
    )
    return _dedup_day(valued)


def _training_status_rel(files: list[str]) -> str:
    """One row per date: the primary training device's entry first, then the latest capture."""
    ts_map = _TS_MAP.replace("j ", "p.j ")
    entries = (
        f"SELECT p.filename AS filename, k AS device_key, {ts_map} -> k AS e "
        f"FROM ({payloads_relation_sql(files)}) AS p, UNNEST(json_keys({ts_map})) AS t(k) "
        f"WHERE json_type({ts_map}) = 'OBJECT'"
    )
    typed = f"""
        SELECT TRY_CAST(json_extract_string(e, '$.calendarDate') AS DATE) AS snapshot_date,
            TRY_CAST(json_extract_string(e, '$.trainingStatus') AS INTEGER)
                AS training_status_code,
            TRY_CAST(json_extract_string(e, '$.weeklyTrainingLoad') AS INTEGER)
                AS weekly_training_load,
            json_extract_string(e, '$.trainingStatusFeedbackPhrase') AS training_status_phrase,
            TRY_CAST(coalesce(json_extract_string(e, '$.deviceId'), device_key) AS BIGINT)
                AS training_device_id,
            coalesce(TRY_CAST(json_extract_string(e, '$.primaryTrainingDevice') AS BOOLEAN), false)
                AS _primary,
            TRY_CAST(regexp_extract(filename, '_([0-9]{{13}})_', 1) AS BIGINT) AS _fetched_ms
        FROM ({entries})
    """
    return f"""
        SELECT * EXCLUDE (_primary, _fetched_ms, _rn) FROM (
            SELECT *, row_number() OVER (
                PARTITION BY snapshot_date
                ORDER BY _primary DESC, _fetched_ms DESC NULLS LAST
            ) AS _rn
            FROM ({typed}) WHERE snapshot_date IS NOT NULL
        ) WHERE _rn = 1
    """


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


# @spec SILVER-DAILY-FITNESS-001, SILVER-DAILY-FITNESS-002, SILVER-DAILY-FITNESS-003
# @spec SILVER-DAILY-FITNESS-004
def fitness_sql(
    max_metrics_files: list[str],
    training_status_files: list[str],
    race_predictions_files: list[str],
) -> str:
    """One row per snapshot day, spine-joining the three Garmin fitness collections."""
    run = _vo2max_rel(max_metrics_files, "generic", "vo2max_running")
    cyc = _vo2max_rel(max_metrics_files, "cycling", "vo2max_cycling")
    ts = _training_status_rel(training_status_files)
    rp = _race_predictions_rel(race_predictions_files)
    return f"""
        WITH run AS ({run}), cyc AS ({cyc}), ts AS ({ts}), rp AS ({rp}),
        days AS (
            SELECT snapshot_date FROM run
            UNION SELECT snapshot_date FROM cyc
            UNION SELECT snapshot_date FROM ts
            UNION SELECT snapshot_date FROM rp
        )
        SELECT
            d.snapshot_date,
            run.vo2max_running, cyc.vo2max_cycling,
            ts.training_status_code, ts.weekly_training_load, ts.training_status_phrase,
            ts.training_device_id,
            rp.race_5k_sec, rp.race_10k_sec, rp.race_half_marathon_sec, rp.race_marathon_sec
        FROM days d
        LEFT JOIN run ON run.snapshot_date = d.snapshot_date
        LEFT JOIN cyc ON cyc.snapshot_date = d.snapshot_date
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
