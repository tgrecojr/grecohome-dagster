"""Body-composition silver column mapping (Garmin weigh-ins, single source).

A single-source reduction of the template — one typed row per **weigh-in** (a body
measurement event). Restores the body/weight view the retired InfluxDB dashboard once
served.

Source shape (profiled against live bronze, 2023-10→2026-03, 73 weigh-ins): each
`garmin/daily_weigh_ins` file is ``{startDate, endDate, dateWeightList: [...],
totalAverage}`` covering a trailing window; `dateWeightList` is the array of weigh-in
records. Each record has a stable `samplePk` (the measurement id), `calendarDate` (local
date), `timestampGMT`/`date` (epoch-millis, GMT vs local), and the body-composition
metrics. The window overlaps across files, so a weigh-in recurs — dedup by `samplePk`
keeping the latest fetch (the Garmin idiom). `samplePk` is the unique key; `measured_date`
is informational (rarely two weigh-ins a day).

Units kept canonical SI: weight/bone/muscle mass in **kg** (the source is grams ÷ 1000);
body fat/water as percent; BMI unitless. Gold/dashboard convert kg → lb.
"""

from __future__ import annotations

from grecohome_core.silver import (
    dedup_latest_sql,
    json_date,
    json_num,
    json_str,
    payloads_relation_sql,
)

# Bronze filename carries the 13-digit fetch-millis; latest re-pull wins the dedup.
_FETCHED_MS = r"TRY_CAST(regexp_extract(filename, '_([0-9]{13})_', 1) AS BIGINT)"


def _weighin_relation(payloads_sql: str) -> str:
    """Unnest each file's ``dateWeightList`` array to one row per weigh-in (``w``)."""
    return (
        "SELECT p.filename AS filename, wi AS w "
        f"FROM ({payloads_sql}) AS p, "
        "UNNEST(CAST(p.j -> '$.dateWeightList' AS JSON[])) AS t(wi) "
        "WHERE json_type(p.j -> '$.dateWeightList') = 'ARRAY'"
    )


def body_sql(files: list[str]) -> str:
    """Typed, deduped Garmin weigh-ins — one row per ``sample_pk`` (latest fetch)."""
    rel = _weighin_relation(payloads_relation_sql(files))
    ts_gmt = f"TRY_CAST({json_str('w', 'timestampGMT')} AS BIGINT)"
    typed = f"""
        SELECT
            TRY_CAST({json_str('w', 'samplePk')} AS BIGINT)            AS sample_pk,
            {json_date('w', 'calendarDate')}                           AS measured_date,
            make_timestamp({ts_gmt} * 1000)                            AS measured_ts_utc,
            {json_num('w', 'weight')} / 1000.0                         AS weight_kg,
            {json_num('w', 'bmi')}                                     AS bmi,
            {json_num('w', 'bodyFat')}                                 AS body_fat_pct,
            {json_num('w', 'bodyWater')}                               AS body_water_pct,
            {json_num('w', 'boneMass')} / 1000.0                       AS bone_mass_kg,
            {json_num('w', 'muscleMass')} / 1000.0                     AS muscle_mass_kg,
            TRY_CAST({json_str('w', 'physiqueRating')} AS INTEGER)     AS physique_rating,
            TRY_CAST({json_str('w', 'visceralFat')} AS INTEGER)        AS visceral_fat,
            TRY_CAST({json_str('w', 'metabolicAge')} AS INTEGER)       AS metabolic_age,
            {json_num('w', 'weightDelta')} / 1000.0                    AS weight_delta_kg,
            {json_str('w', 'sourceType')}                              AS source_type,
            {_FETCHED_MS}                                              AS _fetched_ms
        FROM ({rel})
        WHERE {json_str('w', 'samplePk')} IS NOT NULL
    """
    deduped = dedup_latest_sql(typed, partition_key="sample_pk", order_by="_fetched_ms")
    return f"SELECT * EXCLUDE (_fetched_ms) FROM ({deduped})"


def bronze_weighin_count_sql(files: list[str]) -> str:
    """Count of distinct bronze weigh-ins (distinct ``samplePk``) — for coverage."""
    rel = _weighin_relation(payloads_relation_sql(files))
    pk = f"TRY_CAST({json_str('w', 'samplePk')} AS BIGINT)"
    return f"SELECT count(DISTINCT {pk}) AS n FROM ({rel})"
