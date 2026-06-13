"""Weather/soil silver column mapping (NOAA USCRN hourly, single source).

A line-oriented reduction of the silver template: read the raw USCRN hourly rows from
bronze, type + deduplicate to one row per hourly observation, write Parquet. No join
(one station, one source).

Source shape (profiled against live bronze, 2010-present, ~144k rows): the CRNH0203
"hourly02" product — fixed-width, **whitespace-delimited, headerless** text, exactly
**38 fields** per line. Bronze stores each UTC day's rows as raw text (see
``grecohome_soil``), so a collection is many ``.txt`` files. Two facts drive the design:

* **The observation's identity is its UTC instant** — ``(UTC_DATE, UTC_TIME)`` (fields
  2-3). A filling day is re-captured a few times, so the same hour appears in several
  files (~169 duplicate rows across the archive); we **dedup on the UTC instant**, latest
  capture winning (the bronze filename carries the 13-digit fetch-millis). Identical
  re-captures carry identical values, so dedup is lossless.
* **The local day is derived here, not trusted from the file.** Per the layer contract,
  UTC lives in bronze and local-day semantics live in silver. We derive ``obs_ts_local``
  from the UTC instant via the station timezone (DST-aware, DuckDB ICU ``AT TIME ZONE``),
  so the gold daily rollup can group by the gardener's wall-clock day.

Units are kept **canonical SI** (°C, mm, W/m², m³/m³, %) — silver is a faithful typed
projection. Imperial + derived gardening metrics (°F, inches, growing-degree-days) live
in the gold daily mart. Sentinels (``-9999`` temps/precip/RH, ``-99`` soil moisture,
``-99999`` solar) become NULL. Leading NUL-byte corruption (seen on 2 DST-transition
rows) is stripped before the whitespace split.
"""

from __future__ import annotations

from grecohome_core.silver import dedup_latest_sql, text_lines_relation_sql

# Sentinel "missing"/error values in the CRNH0203 product, mapped to NULL on type.
# Temperatures use -9999.0 for missing; the IR surface-temperature sensor additionally
# reports 99999.0 as an error code (observed on 2011-04-14 18:00Z with SUR_TEMP_TYPE=3),
# so both are nulled. Applying 99999.0 to all temp fields is harmless (it's physically
# impossible) and guards any field that picks up the same error code.
_TEMP_SENTINELS = (-9999.0, 99999.0)  # air/surface/soil temperatures, precip, RH
_MOISTURE_SENTINELS = (-99.0,)  # volumetric soil moisture (m³/m³)
_SOLAR_SENTINELS = (-99999.0,)  # solar radiation (W/m²)

# Every numeric measurement: (output column, 1-based field index, missing sentinels).
# Field positions per the product's HEADERS.txt, confirmed against live bronze.
_FIELDS: list[tuple[str, int, tuple[float, ...]]] = [
    ("air_temp_c", 10, _TEMP_SENTINELS),  # T_HR_AVG
    ("air_temp_max_c", 11, _TEMP_SENTINELS),  # T_MAX
    ("air_temp_min_c", 12, _TEMP_SENTINELS),  # T_MIN
    ("precip_mm", 13, _TEMP_SENTINELS),  # P_CALC (hourly total)
    ("solar_rad_wm2", 14, _SOLAR_SENTINELS),  # SOLARAD
    ("surface_temp_c", 21, _TEMP_SENTINELS),  # SUR_TEMP
    ("surface_temp_max_c", 23, _TEMP_SENTINELS),  # SUR_TEMP_MAX
    ("surface_temp_min_c", 25, _TEMP_SENTINELS),  # SUR_TEMP_MIN
    ("rh_pct", 27, _TEMP_SENTINELS),  # RH_HR_AVG
    ("soil_moisture_5", 29, _MOISTURE_SENTINELS),  # SOIL_MOISTURE_5
    ("soil_moisture_10", 30, _MOISTURE_SENTINELS),
    ("soil_moisture_20", 31, _MOISTURE_SENTINELS),
    ("soil_moisture_50", 32, _MOISTURE_SENTINELS),
    ("soil_moisture_100", 33, _MOISTURE_SENTINELS),
    ("soil_temp_5", 34, _TEMP_SENTINELS),  # SOIL_TEMP_5
    ("soil_temp_10", 35, _TEMP_SENTINELS),
    ("soil_temp_20", 36, _TEMP_SENTINELS),
    ("soil_temp_50", 37, _TEMP_SENTINELS),
    ("soil_temp_100", 38, _TEMP_SENTINELS),
]

# The UTC instant of an observation = strptime(UTC_DATE || UTC_TIME). This is the
# dedup key and the basis for the derived local timestamp.
_OBS_TS_UTC = "TRY_CAST(strptime(f[2] || f[3], '%Y%m%d%H%M') AS TIMESTAMP)"

# Bronze filename carries the 13-digit fetch-millis; latest capture wins an instant's
# tie-break (NULLs sort last in dedup_latest_sql).
_FETCHED_MS = r"TRY_CAST(regexp_extract(filename, '_([0-9]{13})_', 1) AS BIGINT)"


def _num(idx: int, sentinels: tuple[float, ...]) -> str:
    """Field ``idx`` (1-based) as DOUBLE, with each missing-sentinel mapped to NULL."""
    expr = f"TRY_CAST(f[{idx}] AS DOUBLE)"
    for sentinel in sentinels:
        expr = f"nullif({expr}, {sentinel})"
    return expr


def _split_sql(files: list[str]) -> str:
    """Rows of ``(filename, f)`` where ``f`` is the 38-element whitespace-split line.

    NUL bytes (seen on 2 DST-transition rows) are stripped before the split so the
    leading WBANNO field stays clean.
    """
    raw = text_lines_relation_sql(files)
    return (
        "SELECT filename, "
        r"regexp_split_to_array(trim(replace(line, chr(0), '')), '\s+') AS f "
        f"FROM ({raw})"
    )


def weather_sql(files: list[str], *, timezone: str) -> str:
    """Typed, deduped USCRN hourly weather — one row per UTC observation instant."""
    tz = timezone.replace("'", "''")
    obs_local = f"(({_OBS_TS_UTC}) AT TIME ZONE 'UTC') AT TIME ZONE '{tz}'"
    measurements = ",\n            ".join(
        f"{_num(idx, sentinels)} AS {name}" for name, idx, sentinels in _FIELDS
    )
    typed = f"""
        SELECT
            {_OBS_TS_UTC}                                   AS obs_ts_utc,
            TRY_CAST(strptime(f[2], '%Y%m%d') AS DATE)      AS obs_date_utc,
            {obs_local}                                     AS obs_ts_local,
            ({obs_local})::DATE                             AS obs_date_local,
            f[1]                                            AS wbanno,
            {measurements},
            {_FETCHED_MS}                                   AS _fetched_ms
        FROM ({_split_sql(files)})
        WHERE {_OBS_TS_UTC} IS NOT NULL
    """
    deduped = dedup_latest_sql(typed, partition_key="obs_ts_utc", order_by="_fetched_ms")
    return f"SELECT * EXCLUDE (_fetched_ms) FROM ({deduped})"


def bronze_obs_count_sql(files: list[str]) -> str:
    """Count of distinct bronze observations (distinct UTC instants) — coverage check."""
    return (
        f"SELECT count(DISTINCT {_OBS_TS_UTC}) AS n "
        f"FROM ({_split_sql(files)}) WHERE {_OBS_TS_UTC} IS NOT NULL"
    )
