# Gold layer

The gold layer is **analysis** — derived marts that answer questions, built from silver. Where
silver is one faithful typed row per event, gold joins across sources and rolls up to a useful
grain (per day, per week, …). Gold reads `SILVER_ROOT` Parquet and writes marts under `GOLD_ROOT`;
like silver it is *derived and fully rebuildable*, and it never writes under `SILVER_ROOT`. Silver
(and bronze beneath it) remain the sources of truth.

Tables today:
- **`gold_daily_wellness`** — one row per local day joining sleep + recovery + daily workout load +
  daily glucose summary. The spine other analyses build on. See [Daily wellness](#daily-wellness)
  below.
- **`gold_daily_weather`** — one row per local day rolled up from `silver_weather`, with the
  imperial + derived **gardening** metrics (°F, inches, growing-degree-days, frost flags). See
  [Daily weather](#daily-weather) below.

## Invariants

- **Rebuildable, not precious.** Every materialization **fully overwrites** its output. Gold is a
  pure projection of *current* silver.
- **Outside silver.** Gold writes only under `GOLD_ROOT`; the atomic writer **refuses** any path
  inside `SILVER_ROOT` (the shared `write_parquet_atomic` guard, `protected_root=SILVER_ROOT`).
  Silver is mounted read-only in the gold container.
- **Own code location / image.** `packages/gold` (`grecohome_gold`) is its own gRPC code location,
  mirroring the layer-per-image pattern (bronze subjects, silver). Depends only on
  `grecohome-core` + DuckDB.
- **Cross-location lineage.** The mart declares its silver upstreams by `AssetKey`; reads are
  filesystem reads of `SILVER_ROOT`, not gRPC calls.
- **Off the API pools**, runs after silver.

## Storage & format

- **Parquet** (zstd), via DuckDB `COPY`, written atomically (temp + `os.replace`).
- **Layout** under `GOLD_ROOT`:

```
{GOLD_ROOT}/wellness/daily_wellness.parquet   # one row per local day (health)
{GOLD_ROOT}/weather/daily_weather.parquet     # one row per local day (weather/gardening)
```

# Daily wellness

`gold_daily_wellness` is the foundational mart — **one row per local calendar day** over a
**continuous date spine** (so gaps are explicit for rolling/streak analysis), left-joining the four
silver tables:

| Source | Join | Aggregation |
|---|---|---|
| `silver_sleep` | `night_date = day` (1:1) | none (curated Garmin + Whoop columns) |
| `silver_recovery` | `recovery_date = day` | deduped to one/day (latest `created_at`) |
| `silver_workouts` | `activity_date = day` | per-day: count + total duration/distance/calories |
| `silver_glucose` | `reading_date = day` | per-day: mean / min / max / std + time-in-range |

Notes:
- **Recovery dedup.** Recovery's `recovery_date` is the UTC date of `created_at` and is occasionally
  2-per-date, so the mart keeps the latest `created_at` per day. (The precise per-sleep link lives
  in `silver_recovery` via `sleep_id`/`cycle_id`.)
- **Glucose time-in-range** uses **70–140 mg/dL** (non-diabetic / metabolic-health band):
  `glucose_tir_pct`, with `glucose_pct_below` (<70) and `glucose_pct_above` (>140).
- **Workouts** can be many per day → aggregated; `workout_count` is 0 (not null) on days with none.
- **Provenance:** `has_sleep` / `has_recovery` / `has_workout` / `has_glucose` make every null
  explainable.

### Schema (selected columns)
`day` (DATE, PK); sleep: `garmin_sleep_score`, `garmin_total_min`, `garmin_rhr`,
`whoop_performance_pct`, `whoop_efficiency_pct`; recovery: `recovery_score`,
`resting_heart_rate`, `hrv_rmssd_milli`, `spo2_percentage`; workouts: `workout_count`,
`workout_total_min`, `workout_distance_km`, `workout_calories`; glucose: `glucose_readings`,
`glucose_mean`, `glucose_min`, `glucose_max`, `glucose_std`, `glucose_tir_pct`,
`glucose_pct_below`, `glucose_pct_above`; `has_*` flags.

### Asset checks
| Check | Severity | What |
|---|---|---|
| `wellness_day_unique_nonnull` | ERROR | one row per `day`, never null |
| `wellness_value_ranges` | ERROR | TIR 0–100, glucose mean / recovery score / counts in bounds |
| `wellness_coverage` | WARN | per-source day coverage; fails only if the mart is empty |

# Daily weather

`gold_daily_weather` is the gardening-facing mart — **one row per local day** over a **continuous
date spine** (gaps explicit), rolled up from `silver_weather` grouped by `obs_date_local`. Where
silver is faithful hourly **SI**, gold is the **imperial + derived** layer other applications
(e.g. a gardening app) consume.

| Metric | Derivation |
|---|---|
| `air_temp_max_f` / `air_temp_min_f` / `air_temp_avg_f` | daily max/min/mean of the °C columns → °F |
| `gdd50` | growing-degree-days, base 50 °F: `max(0, (Tmax_f + Tmin_f)/2 − 50)` |
| `frost` / `hard_freeze` | daily min ≤ 32 °F / ≤ 28 °F |
| `precip_total_in` | daily total `precip_mm` ÷ 25.4 |
| `solar_rad_mean_wm2` / `solar_rad_max_wm2` | daily mean / max |
| `surface_temp_max_f` / `surface_temp_min_f` | daily surface-temp extremes → °F |
| `rh_mean_pct` | daily mean relative humidity |
| `soil_temp_{5,10,20,50,100}_f_mean` | daily mean soil temp per depth → °F |
| `soil_moisture_{5,10,20,50,100}_mean` | daily mean volumetric soil moisture per depth |
| `hours_observed` | observation count for the day (coverage) |
| `has_weather` | provenance — false on a spine gap day |

Validated against the live archive (6,009 days, 2010-present): ~47.7 in/yr precipitation, 1,676
frost days — consistent with SE Pennsylvania.

### Asset checks
| Check | Severity | What |
|---|---|---|
| `weather_day_unique_nonnull` | ERROR | one row per `day`, never null |
| `weather_value_ranges` | ERROR | imperial temps/soil/RH/precip/GDD/hours in bounds; daily max ≥ min |
| `weather_coverage` | WARN | day coverage; reports frost days; fails only if the mart is empty |

# Operations

## Scheduling
- `gold_wellness_daily` (07:30 UTC) rebuilds the wellness mart **after** silver's daily rebuilds
  (≤ 06:55) and silver checks (07:00).
- `gold_weather_daily` (07:40 UTC) rebuilds the weather mart after `silver_weather`.
- `gold_checks_daily` (08:00 UTC) runs **all** gold checks (wellness + weather) independently.

All off by default; enable in the UI. Rebuild on demand with
`dagster job execute --job gold_wellness_job` (or `--job gold_weather_job`).

## Deployment
See [DEPLOYMENT.md → Gold](DEPLOYMENT.md#gold-cross-layer-marts) and
[ENV_TEMPLATE.md](ENV_TEMPLATE.md): `SILVER_ROOT` mounted **read-only**, `GOLD_ROOT` writable on a
separate volume, reserved `GOLD_MONITOR_DIR`.
