# Silver weather — EARS specs

Design: [weather-design.md](weather-design.md). Prefix `SILVER-WEATHER`. Facets: `TABLE`
(`silver_weather`), `CHECKS`, `SCHED`.

## Table

- [x] **SILVER-WEATHER-TABLE-001**: When `silver_weather` materializes, the system shall read every non-blank line of every `uscrn/hourly` bronze file, strip NUL bytes, trim it, split it on whitespace, and write `{SILVER_ROOT}/weather/silver_weather.parquet` with one row per `obs_ts_utc` holding the columns of the weather design's schema, each measurement taken from its fixed field position.
- [x] **SILVER-WEATHER-TABLE-002**: The `silver_weather` table shall set `obs_ts_utc` from fields 2 and 3 parsed together as `YYYYMMDDHHMM`, `obs_date_utc` from field 2, and `obs_ts_local` and `obs_date_local` by converting `obs_ts_utc` to the `USCRN_TIMEZONE` time zone with daylight-saving rules applied.
- [x] **SILVER-WEATHER-TABLE-003**: If a line has fewer than three fields, then `silver_weather` shall leave it out.
- [x] **SILVER-WEATHER-TABLE-004**: When several bronze lines share an `obs_ts_utc`, `silver_weather` shall keep the one from the capture with the latest fetch time in its bronze filename.
- [x] **SILVER-WEATHER-TABLE-005**: The `silver_weather` table shall set a measurement to null when it equals its missing-value sentinel (−9999 or 99999 for temperatures, precipitation and humidity; −99 for soil moisture; −99999 for solar radiation).
- [x] **SILVER-WEATHER-TABLE-006**: The `silver_weather` table shall keep solar radiation, the three surface temperatures and relative humidity only when their adjacent QC flag field is `0`, setting them null otherwise.
- [x] **SILVER-WEATHER-TABLE-007**: When `silver_weather` materializes, the system shall record `rows`, `bronze_files` and `path` as materialization metadata.

## Asset checks

- [x] **SILVER-WEATHER-CHECKS-001**: The `weather_obs_unique_nonnull` check shall fail at ERROR severity when `obs_ts_utc` has a duplicate or a null, or `obs_date_utc` or `obs_date_local` has a null.
- [x] **SILVER-WEATHER-CHECKS-002**: The `weather_value_ranges` check shall fail at ERROR severity when any non-null temperature is outside −60…60, soil moisture outside 0–1, `rh_pct` outside 0–100, `precip_mm` below 0, or `solar_rad_wm2` outside 0–2000.
- [x] **SILVER-WEATHER-CHECKS-003**: The `weather_coverage_vs_bronze` check shall fail at WARN severity when the table's row count is less than the number of distinct parseable observation instants in `uscrn/hourly` bronze, and shall report the number of distinct `obs_date_local` values.
- [x] **SILVER-WEATHER-CHECKS-004**: The system shall define `silver_weather_freshness` with `table_freshness_check` on `silver_weather`.
- [x] **SILVER-WEATHER-CHECKS-005**: If the `silver_weather` Parquet doesn't exist, then every weather check shall fail at ERROR severity.
- [x] **SILVER-WEATHER-CHECKS-006**: The system shall wrap every weather check in `alerting_check`.

## Schedule

- [x] **SILVER-WEATHER-SCHED-001**: The system shall define `silver_weather_job` over `silver_weather` and its asset checks, and a `silver_weather_daily` schedule that runs it daily at 06:55 UTC with a default status of stopped.
