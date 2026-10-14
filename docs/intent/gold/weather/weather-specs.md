# Gold daily weather — EARS specs

Design: [weather-design.md](weather-design.md). Prefix `GOLD-WEATHER`. Facets: `MART`, `CHECKS`,
`SCHED`.

## Mart

- [x] **GOLD-WEATHER-MART-001**: When `gold_daily_weather` materializes, the system shall write `{GOLD_ROOT}/weather/daily_weather.parquet` with one row per day from the earliest to the latest `silver_weather.obs_date_local`, aggregating that day's hourly observations into the weather design's metrics in °F, inches and the other listed units.
- [x] **GOLD-WEATHER-MART-002**: The `gold_daily_weather` mart shall compute `gdd50` as max(0, (`air_temp_max_f` + `air_temp_min_f`) / 2 − 50), `frost` as `air_temp_min_f` ≤ 32 and `hard_freeze` as `air_temp_min_f` ≤ 28.
- [x] **GOLD-WEATHER-MART-003**: The `gold_daily_weather` mart shall report each day's valid-hour count per field (`air_temp_hours`, `precip_hours`, `solar_hours`, `surface_temp_hours`, `rh_hours`, `soil_temp_{d}_hours`, `soil_moisture_{d}_hours`) and `hours_observed`, each 0 on a day without observations, and `has_weather` as whether `hours_observed` is above 0.
- [x] **GOLD-WEATHER-MART-004**: The `gold_daily_weather` mart shall set each aggregate to null when the valid-hour count that gates it is below `GOLD_WEATHER_MIN_VALID_HOURS` (default 22): `air_temp_hours` gating the air temperatures, GDD and frost flags, `surface_temp_hours` the surface temperatures, and each other field its own count.
- [x] **GOLD-WEATHER-MART-005**: If the `silver_weather` Parquet doesn't exist, then `gold_daily_weather` shall treat it as an empty table and write an empty mart.
- [x] **GOLD-WEATHER-MART-006**: When `gold_daily_weather` materializes, the system shall record `rows`, `gdd_base_f`, `min_valid_hours` and `path` as materialization metadata.

## Asset checks

- [x] **GOLD-WEATHER-CHECKS-001**: The `weather_day_unique_nonnull` check on `gold_daily_weather` shall fail at ERROR severity when `day` has a duplicate or a null.
- [x] **GOLD-WEATHER-CHECKS-002**: The `weather_value_ranges` check on `gold_daily_weather` shall fail at ERROR severity when any non-null air or surface temperature is outside −60…200 °F, soil temperature outside −40…140 °F, soil moisture outside 0–1, `rh_mean_pct` outside 0–100, `precip_total_in`, `gdd50` or a solar value below 0, `hours_observed` outside 0–26, any `*_hours` below 0 or above `hours_observed`, or `air_temp_max_f` below `air_temp_min_f`.
- [x] **GOLD-WEATHER-CHECKS-003**: The `weather_coverage` check on `gold_daily_weather` shall fail at WARN severity when no day has weather, and shall report the days, the days with weather and the frost days.
- [x] **GOLD-WEATHER-CHECKS-004**: The system shall define `gold_daily_weather_freshness` with `table_freshness_check` on `gold_daily_weather`.
- [x] **GOLD-WEATHER-CHECKS-005**: If the `gold_daily_weather` Parquet doesn't exist, then every weather mart check shall fail at ERROR severity.
- [x] **GOLD-WEATHER-CHECKS-006**: The system shall wrap every weather mart check in `alerting_check`.

## Schedule

- [x] **GOLD-WEATHER-SCHED-001**: The system shall define `gold_weather_job` over `gold_daily_weather` and its asset checks, and a `gold_weather_daily` schedule that runs it daily at 07:40 UTC with a default status of stopped.
