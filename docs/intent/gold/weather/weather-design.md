---
parent: gold
prefix: GOLD-WEATHER
---

# Gold daily weather

## Context

`gold_daily_weather` is the gardening-facing mart: **one row per local day**, rolled up from
`silver_weather` by `obs_date_local`, in imperial units with derived gardening metrics. Where
silver is faithful hourly SI, this is what a gardening view consumes.

## Asset

| Asset | Grain | Reads | Writes |
|---|---|---|---|
| `gold_daily_weather` | One row per `day` | `silver_weather` (declared as a dependency) | `{GOLD_ROOT}/weather/daily_weather.parquet` |

Each run records `rows`, `gdd_base_f`, `min_valid_hours` and `path` as metadata.

## Metrics

| Column | Derivation |
|---|---|
| `air_temp_max_f`, `air_temp_min_f`, `air_temp_avg_f` | Daily max of `air_temp_max_c`, min of `air_temp_min_c`, mean of `air_temp_c`, in °F |
| `gdd50` | Growing degree days, base 50 °F: `max(0, (max_f + min_f) / 2 − 50)` |
| `frost`, `hard_freeze` | Daily min ≤ 32 °F, ≤ 28 °F |
| `precip_total_in` | Daily sum of `precip_mm` ÷ 25.4 |
| `solar_rad_mean_wm2`, `solar_rad_max_wm2` | Daily mean and max |
| `surface_temp_max_f`, `surface_temp_min_f` | Daily surface-temperature extremes, in °F |
| `rh_mean_pct` | Daily mean relative humidity |
| `soil_temp_{d}_f_mean`, `soil_moisture_{d}_mean` | Daily mean at each depth `d` of 5, 10, 20, 50, 100 cm (temperature in °F) |
| `hours_observed` | Hours with any of air temperature, precipitation, solar, humidity, or a soil reading; NOAA's all-missing "no transmission" rows don't count |
| `*_hours` | Valid-hour count per field: `air_temp_hours`, `precip_hours`, `solar_hours`, `surface_temp_hours`, `rh_hours`, `soil_temp_{d}_hours`, `soil_moisture_{d}_hours` |
| `has_weather` | Whether the day has any observed hour (false on a spine gap) |

## Completeness gate

Each aggregate is reported **only when its own field has at least `GOLD_WEATHER_MIN_VALID_HOURS`
valid hours that day** (default **22**); otherwise it's null and the `*_hours` column says why.
Hour counts are always present (0 on a gap day). Which count gates what:

- `air_temp_hours` counts hours with `air_temp_c`; it gates the air-temperature max, min and mean,
  GDD and the frost flags (the max and min come from the hourly max and min columns);
- `surface_temp_hours` counts hours with `surface_temp_max_c`; it gates both surface extremes;
- the others gate their own field.

Lower the gate (`GOLD_WEATHER_MIN_VALID_HOURS`) to get values for partial days; the counts stay
the same.

USCRN dropouts cluster (in 2026 the soil probes went missing mostly in the local afternoon), so an
aggregate over the surviving hours is biased, not just imprecise; gating beats averaging what's
there. 22 of 24 is the completeness NCEI applies to its own daily product, and it tolerates the
23-hour spring-forward day.

## The spine

The spine runs from the first to the last `obs_date_local` in silver; every day in between is a
row. The first and last days can be partial (the station's first record, today), and the gate
nulls their aggregates.

## Asset checks

| Check | Severity | Passes when |
|---|---|---|
| `weather_day_unique_nonnull` | ERROR | `day` is unique and never null |
| `weather_value_ranges` | ERROR | Air and surface temperatures within −60…200 °F; soil temperatures −40…140 °F; soil moisture 0–1; `rh_mean_pct` 0–100; `precip_total_in`, `gdd50` and solar ≥ 0; `hours_observed` 0–26; every `*_hours` between 0 and `hours_observed`; air max ≥ air min |
| `weather_coverage` | WARN | At least one day has weather; reports days, days with weather and frost days |

Every check fails at ERROR, and pages, if the mart's Parquet doesn't exist.

## Schedule

`gold_weather_job` rebuilds the mart and runs its checks daily at **07:40 UTC**
(`gold_weather_daily`, which starts stopped), after `silver_weather` (06:55) and silver's checks.

## Decisions & Alternatives

| Decision | Chosen | Alternatives Considered | Rationale |
|---|---|---|---|
| Units | Imperial plus derived metrics | Keep SI | Silver keeps SI; gold serves the gardener. |
| Partial days | Gate each aggregate on its field's valid hours | Average the hours present | Dropouts cluster by time of day, biasing partial aggregates. |
| Gate default | 22 of 24 hours | 24; 18 | Matches NCEI's daily product; tolerates the DST day. |
| GDD base | 50 °F | 40 °F, 45 °F | The common base for warm-season plants and lawn. |

## References

- `packages/gold/src/grecohome_gold/daily_weather.py`, `dagster/weather_assets.py`,
  `dagster/weather_checks.py`
- [Silver weather](../../silver/weather/weather-design.md)
