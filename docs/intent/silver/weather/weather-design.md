---
parent: silver
prefix: SILVER-WEATHER
---

# Silver weather

## Context

`silver_weather` is the typed projection of the NOAA USCRN station's hourly observations: **one
row per hourly observation**, in SI units. It reads `uscrn/hourly` bronze, where each capture is
raw text of the CRNH0203 "hourly02" product: **headerless, whitespace-delimited** lines of
exactly **38 fields**, one station (WBANNO 03761, PA_Avondale_2_N), from 2010.

Silver stays a faithful projection: imperial units and derived gardening metrics (°F, inches,
growing degree days, frost) belong to the gold daily weather mart, which groups by the local day
defined here.

## Assets

| Asset | Grain | Reads | Writes |
|---|---|---|---|
| `silver_weather` | One row per UTC observation instant | `uscrn/hourly` (bronze dependency `uscrn_bronze_hourly`) | `{SILVER_ROOT}/weather/silver_weather.parquet` |

Each run records `rows`, `bronze_files` and `path` as metadata.

## Reading the lines

The core line reader yields each physical line whole. Silver strips NUL bytes (seen on two rows
at daylight-saving transitions), trims the line and splits it on whitespace; fields are then
picked **by position** (1-based, per the product's `HEADERS.txt`).

## The observation instant and the local day

- **`obs_ts_utc`**, the observation's identity, is `UTC_DATE` (field 2) and `UTC_TIME` (field 3)
  parsed together. A line too short to have them (fewer than three fields) is dropped; a line
  whose date or time is present but malformed fails the rebuild, since the parse raises.
- **`obs_date_utc`** is the UTC date (field 2).
- **`obs_ts_local`** and **`obs_date_local`** come from converting the UTC instant to the station's
  time zone (`USCRN_TIMEZONE`, default `America/New_York`), daylight-saving aware (DuckDB's ICU
  extension). So `obs_date_local` can differ from `obs_date_utc`: an 02:00 UTC reading belongs to
  the previous local evening. The local day is derived here, never taken from the bronze
  partition (bronze partitions are UTC fetch slices). `obs_ts_local` isn't unique: the repeated
  hour at a daylight-saving fall-back maps two instants to the same local time.
- **`wbanno`** is the station id (field 1). The table assumes **one station**: `wbanno` isn't part
  of the key, and the time zone is a single setting. Adding a station would need a key and time-zone
  decision first.
- A line with fewer than 38 fields is kept, with its missing positions null; every line has had
  exactly 38.

## Deduplication

**One row per `obs_ts_utc`**, keeping the **latest capture** (the 13-digit fetch time in the
bronze filename). A filling day is captured a few times, and NOAA's corrections are re-captured
weekly, so the same hour appears in several files. Identical re-captures carry identical values;
a correction carries the corrected value, which the latest capture supplies.

## Values

Every measurement is a `DOUBLE`, in **canonical SI** (°C, mm, W/m², m³/m³, %).

- **Missing-value sentinels become `NULL`:** −9999 for temperatures, precipitation and humidity;
  −99 for soil moisture; −99999 for solar radiation. The surface-temperature sensor also reports
  99999 as an error code, so 99999 is nulled on every field that uses −9999 (temperatures,
  precipitation and humidity).
- **QC flags are honored.** Solar radiation, the three surface temperatures and relative humidity
  each carry an adjacent QC flag. A value is kept only when its flag is `0`; any other flag
  (including an unparseable one) nulls it. A failed-QC value is not a sentinel (USCRN once reported
  solar radiation of −13107 with flag 3), so without this it would leak an impossible reading.
  The other fields have no flag in this product and get sentinel handling only.
- An observation whose every measurement is missing is kept, all nulls.

## Schema

| Column | Field | Meaning |
|---|---|---|
| `obs_ts_utc` | 2 + 3 | UTC instant (TIMESTAMP): the key |
| `obs_date_utc` | 2 | UTC date (DATE) |
| `obs_ts_local`, `obs_date_local` | (derived) | Local time (TIMESTAMP) and date (DATE), station time zone |
| `wbanno` | 1 | Station id (VARCHAR) |
| `air_temp_c`, `air_temp_max_c`, `air_temp_min_c` | 10, 11, 12 | `T_HR_AVG`, `T_MAX`, `T_MIN` |
| `precip_mm` | 13 | `P_CALC`, the hourly total |
| `solar_rad_wm2` | 14 (flag 15) | `SOLARAD` |
| `surface_temp_c`, `surface_temp_max_c`, `surface_temp_min_c` | 21, 23, 25 (flags 22, 24, 26) | Infrared surface temperature |
| `rh_pct` | 27 (flag 28) | `RH_HR_AVG` |
| `soil_moisture_5` … `soil_moisture_100` | 29–33 | Volumetric soil moisture at 5, 10, 20, 50, 100 cm |
| `soil_temp_5` … `soil_temp_100` | 34–38 | Soil temperature at 5, 10, 20, 50, 100 cm |

## Asset checks

| Check | Severity | Passes when |
|---|---|---|
| `weather_obs_unique_nonnull` | ERROR | `obs_ts_utc` is unique, and neither it, `obs_date_utc` nor `obs_date_local` is null |
| `weather_value_ranges` | ERROR | Every non-null temperature is within −60…60 °C, soil moisture within 0–1, humidity 0–100, precipitation ≥ 0, solar radiation 0–2000 |
| `weather_coverage_vs_bronze` | WARN | Silver's row count ≥ bronze's distinct parseable instants; also reports distinct local days |
| `silver_weather_freshness` | ERROR | The Parquet was written within the last 36 hours |

Every check fails at ERROR, and pages, if the table's Parquet doesn't exist. Gaps in the station's
record (sensor faults) are reported by bronze's completeness check, not here.

## Schedule

`silver_weather_job` rebuilds `silver_weather` and runs its checks daily at **06:55 UTC**
(`silver_weather_daily`, which starts stopped), after USCRN's 6-hourly captures.

## Decisions & Alternatives

| Decision | Chosen | Alternatives Considered | Rationale |
|---|---|---|---|
| Parsing | Whole lines, split on whitespace, fields by position | A fixed-width or CSV reader | The product is headerless and whitespace-delimited; positions are fixed by the product spec. |
| Local day | Derived from the UTC instant via the station's IANA zone | The file's local date fields; the bronze partition | Bronze is UTC; local semantics live in silver, DST-aware. |
| Units | Canonical SI | Imperial | Silver is a faithful projection; gold converts for gardening. |
| QC flags | Null a flagged value unless its flag is 0 | Sentinels only | A failed-QC value is a real-looking number, not a sentinel; it would leak impossible readings. |
| Dedup | Latest capture per instant | First capture | Re-captures pick up NOAA corrections. |

## References

- `packages/silver/src/grecohome_silver/weather.py`, `dagster/weather_assets.py`,
  `dagster/weather_checks.py`
- [Soil/USCRN design](../../soil/soil-design.md): the capture, corrections and station
