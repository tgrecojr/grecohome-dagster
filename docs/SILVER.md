# Silver layer

The silver layer's intent (layer rules, architecture, storage, check and schedule conventions)
lives in [docs/intent/silver/silver-design.md](intent/silver/silver-design.md), and the shared
helpers in [docs/intent/core/silver/silver-design.md](intent/core/silver/silver-design.md).
Sleep, glucose, cycles (recovery, strain) and workouts are specified under
[docs/intent/silver/](intent/silver/silver-design.md).

This page still describes the table families not yet specified there:

- **Weather** — `silver_weather` (per-hour NOAA USCRN soil/weather). The first
  **line-oriented** (fixed-width text) source. See [Weather (NOAA USCRN)](#weather-noaa-uscrn)
  below.
- **Daily** — `silver_daily` (per local day Garmin movement + wellness rollup). See
  [Daily summary (Garmin)](#daily-summary-garmin) below.
- **Body** — `silver_body` (per-weigh-in Garmin body composition). See
  [Body (Garmin weigh-ins)](#body-garmin-weigh-ins) below.
- **Fitness** — `silver_fitness` (per-snapshot-day Garmin VO2max / training status / race
  predictions; the first **multi-collection** table). See [Fitness (Garmin)](#fitness-garmin)
  below.

Layout under `SILVER_ROOT` for these families (`<family>/<table>.parquet`):

```
{SILVER_ROOT}/weather/silver_weather.parquet # weather: one row per USCRN hourly observation
{SILVER_ROOT}/daily/silver_daily.parquet     # daily summary: one row per local day (Garmin)
{SILVER_ROOT}/body/silver_body.parquet       # body: one row per Garmin weigh-in
{SILVER_ROOT}/fitness/silver_fitness.parquet # fitness: one row per Garmin snapshot day
```

# Weather (NOAA USCRN)

`silver_weather` is a single-source table — one row per hourly observation — and the first
**line-oriented** source (fixed-width text, not JSON or CSV-with-header).

### Source & event date
USCRN bronze (`uscrn/hourly`) is the CRNH0203 "hourly02" product: **headerless,
whitespace-delimited** text, exactly **38 fields** per line (profiled against live bronze:
2010-present, ~144k rows, one station — WBANNO 03761, PA_Avondale_2_N). The generic
`grecohome_core.silver.text_lines_relation_sql` reads each line whole (delimiter = ASCII Unit
Separator, which never occurs); the **field mapping** lives in `grecohome_silver.weather`, which
`regexp_split_to_array`s the line and picks fields by position. An observation's identity is its
**UTC instant** — `strptime(UTC_DATE || UTC_TIME)`.

The **local day is derived here**, not trusted from the file: `obs_ts_local` is the UTC instant
converted through the station timezone (`USCRN_TIMEZONE`, default `America/New_York`), **DST-aware**
via DuckDB's ICU `AT TIME ZONE`. So `obs_date_local` can differ from `obs_date_utc` (a 02:00 UTC
reading is the previous local evening). The gold daily mart groups by `obs_date_local`.

### Deduplication
A filling day is re-captured a few times, so the same hour appears in several files (~169 duplicate
rows across the archive). Dedup keys on **`obs_ts_utc`**, latest capture winning (the 13-digit
fetch-millis in the bronze filename). Identical re-captures carry identical values, so dedup is
lossless. NUL-byte corruption (seen on 2 DST-transition rows) is stripped before the split.

### Typing & units — canonical SI
Sentinels become NULL: **−9999** (temps/precip/RH), **−99** (soil moisture), **−99999** (solar).
Units stay **canonical SI** (°C, mm, W/m², m³/m³, %) — silver is a faithful typed projection; the
imperial + derived gardening metrics (°F, inches, growing-degree-days) live in the gold daily mart.
A fully-sentinel observation is kept with every measurement null (never dropped).

### Schema (`silver_weather`)
One row per hourly observation.

| Column | Type | Note |
|---|---|---|
| `obs_ts_utc` | TIMESTAMP | UTC instant — the dedup key / canonical identity |
| `obs_date_utc` | DATE | UTC date |
| `obs_ts_local` | TIMESTAMP | local wall-clock (station tz, DST-aware) |
| `obs_date_local` | DATE | local date (the gardener's day; gold groups by this) |
| `wbanno` | VARCHAR | station id |
| `air_temp_c` / `air_temp_max_c` / `air_temp_min_c` | DOUBLE | `T_HR_AVG` / `T_MAX` / `T_MIN` |
| `precip_mm` | DOUBLE | `P_CALC` (hourly total) |
| `solar_rad_wm2` | DOUBLE | `SOLARAD` |
| `surface_temp_c` / `surface_temp_max_c` / `surface_temp_min_c` | DOUBLE | infrared surface temp |
| `rh_pct` | DOUBLE | `RH_HR_AVG` |
| `soil_moisture_5` … `soil_moisture_100` | DOUBLE | volumetric (m³/m³) at 5/10/20/50/100 cm |
| `soil_temp_5` … `soil_temp_100` | DOUBLE | °C at 5/10/20/50/100 cm |

### Asset checks
| Check | Severity | What |
|---|---|---|
| `weather_obs_unique_nonnull` | ERROR | one row per `obs_ts_utc`; UTC + local day keys non-null |
| `weather_value_ranges` | ERROR | temps −60…60 °C, soil moisture 0–1, RH 0–100, precip ≥ 0, solar 0–2000 |
| `weather_coverage_vs_bronze` | WARN | silver obs ≈ bronze distinct instants; reports distinct local days |

# Daily summary (Garmin)

`silver_daily` is a single-source table — one typed row per **local day** — the daily
movement-and-wellness rollup.

### Source & event date
`garmin/user_summary` is a flat daily **super-object**: one record already carries steps,
distance, every calorie type, floors, intensity minutes, resting/min/max HR, the full
stress breakdown, body-battery, SpO2, and respiration. So this one table subsumes the
standalone per-metric daily collections (`daily_steps`, `floors`, `intensity_minutes`,
`resting_heart_rate`, daily `stress`/`spo2`/`respiration`/`body_battery`) — those remain
only for *intraday* detail. The event date is `calendarDate` (authoritative, already
local). Garmin re-pulls a day into several files → dedup by `calendarDate` keeping the
latest fetch (the 13-digit `fetched_ms`), the Garmin sleep idiom.

### Typing & units
Canonical to the source: distance in metres, calories in kilocalories, durations in
seconds, intensity in minutes. Garmin's `-1`/`-2` "no-data" sentinels on the stress
*levels* become NULL. Coverage varies by day (older days predate some sensors); a missing
field is NULL, the day is kept.

### Schema (`silver_daily`) — selected columns
One row per `activity_date` (DATE, key). Movement: `total_steps`, `step_goal`,
`total_distance_m`, `total_kilocalories`, `active_kilocalories`, `bmr_kilocalories`,
`floors_ascended`/`_descended`, `moderate_intensity_min`, `vigorous_intensity_min`. Heart:
`min_heart_rate`, `max_heart_rate`, `resting_heart_rate`, `avg7d_resting_heart_rate`.
Stress: `avg_stress_level`, `max_stress_level`, `rest/low/medium/high_stress_duration`.
Body battery: `body_battery_high`/`_low`/`_charged`/`_drained`. Vitals: `avg_spo2`,
`lowest_spo2`, `avg_waking_respiration`, `highest`/`lowest_respiration`. Time-in-state:
`highly_active`/`active`/`sedentary`/`sleeping_seconds`.

### Asset checks
| Check | Severity | What |
|---|---|---|
| `daily_date_unique_nonnull` | ERROR | one row per `activity_date`, never null |
| `daily_value_ranges` | ERROR | steps/calories/HR/stress/SpO2/body-battery within generous bounds |
| `daily_coverage_vs_bronze` | WARN | silver days ≈ bronze distinct `calendarDate`; reports days-with-steps |

# Body (Garmin weigh-ins)

`silver_body` is a single-source table — one row per **weigh-in** (a body measurement
event) — restoring the body/weight view the retired InfluxDB dashboard once served.

### Source & event date
`garmin/daily_weigh_ins` is `{startDate, endDate, dateWeightList: […], totalAverage}` over
a trailing window; `dateWeightList` is the array of weigh-in records, each with a stable
`samplePk` (the measurement id), `calendarDate` (local date), `timestampGMT` (epoch-millis,
the UTC instant), and body-composition metrics. The window overlaps across files so a
weigh-in recurs → dedup by `samplePk` keeping the latest fetch. `samplePk` is the unique
key; `measured_date` is informational (rarely two weigh-ins a day). Weigh-ins are sparse
(not daily).

### Typing & units
Canonical **SI**: weight / bone / muscle mass in **kg** (source grams ÷ 1000), body fat /
water as percent, BMI unitless. Gold/dashboard convert kg → lb.

### Schema (`silver_body`)
One row per weigh-in: `sample_pk` (BIGINT, key), `measured_date` (DATE), `measured_ts_utc`
(TIMESTAMP), `weight_kg`, `bmi`, `body_fat_pct`, `body_water_pct`, `bone_mass_kg`,
`muscle_mass_kg` (DOUBLE), `physique_rating`, `visceral_fat`, `metabolic_age` (INT),
`weight_delta_kg` (DOUBLE), `source_type` (VARCHAR).

### Asset checks
| Check | Severity | What |
|---|---|---|
| `body_sample_unique_nonnull` | ERROR | one row per `sample_pk`; `sample_pk`/`measured_date` non-null |
| `body_value_ranges` | ERROR | weight 20–300 kg, BMI 10–80, fat/water/muscle bounded (catches grams-vs-kg) |
| `body_coverage_vs_bronze` | WARN | silver weigh-ins ≈ bronze distinct `samplePk`; reports distinct days |

# Fitness (Garmin)

`silver_fitness` is the first **multi-collection** table — one row per **snapshot day**,
joining three Garmin *current-state snapshot* collections.

### Sources & event date
| Collection | Fields |
|---|---|
| `garmin/max_metrics` | VO2max `generic.vo2MaxValue` (running) + `cycling.vo2MaxValue` |
| `garmin/training_status` | the device-keyed `latestTrainingStatusData` map → `trainingStatus` (int code), `weeklyTrainingLoad`, feedback phrase |
| `garmin/race_predictions` | `time5K` / `time10K` / `timeHalfMarathon` / `timeMarathon` (seconds) |

**Date = the snapshot day = the bronze `dt` partition.** Unlike the event-based tables, these
are *current-value-carried-until-it-changes* snapshot endpoints — the payload carries no deep
history (`max_metrics` has no date; `race_predictions`' `calendarDate` only moves when the
prediction changes), so the meaningful day is **when the snapshot was taken** (`dt`). This is
the one deliberate exception to "`dt` ≠ event date" — justified because the value *is* a daily
state. Each collection is deduped to the **latest capture per `dt`**, then the three are
spine-joined on the day (a day in any collection yields a row; the others null).

### Coverage (sparse, growing)
The Garmin capture began **2026-06-03** and these endpoints don't backfill, so history starts
there and grows ~1 day/day; VO2max only changes on run/ride days. Silver is rebuildable, so a
later rebuild reprocesses all of bronze. `endurance_score` / `hill_score` are omitted (their
values sit in nested windowed DTO lists with the accessible top-level fields null).

### Schema (`silver_fitness`)
One row per `snapshot_date` (DATE, key): `vo2max_running`, `vo2max_cycling` (DOUBLE),
`training_status_code` (INT), `weekly_training_load` (INT), `training_status_phrase` (VARCHAR),
`race_5k_sec`, `race_10k_sec`, `race_half_marathon_sec`, `race_marathon_sec` (INT).

### Asset checks
| Check | Severity | What |
|---|---|---|
| `fitness_day_unique_nonnull` | ERROR | one row per `snapshot_date`, never null |
| `fitness_value_ranges` | ERROR | VO2max 10–90, weekly load ≥ 0, race times > 0 |
| `fitness_coverage` | WARN | per-metric coverage (sparse expected); fails only if the table is empty |

# Operations

## Scheduling

- `silver_sleep_daily` (06:00 UTC) rebuilds the three sleep assets; see the
  [sleep design](intent/silver/sleep/sleep-design.md).
- `silver_glucose_daily` (06:30), `silver_recovery_daily` (06:50) and `silver_strain_daily` (06:52
  UTC); see the [glucose](intent/silver/glucose/glucose-design.md) and
  [cycles](intent/silver/cycles/cycles-design.md) designs.
- `silver_workouts_daily` (06:45), `silver_workout_splits_daily` (06:47) and
  `silver_whoop_workouts_daily` (06:53 UTC); see the [workouts design](intent/silver/workouts/workouts-design.md).
- `silver_weather_daily` (06:55 UTC) rebuilds `silver_weather` (USCRN captured a few times a day;
  a daily rebuild keeps silver a current projection).
- `silver_daily_daily` (06:40 UTC) rebuilds `silver_daily` (after the Garmin daily capture).
- `silver_body_daily` (06:42 UTC) rebuilds `silver_body` (Garmin weigh-ins).
- `silver_fitness_daily` (06:44 UTC) rebuilds `silver_fitness` (Garmin snapshot collections).
- `silver_checks_daily` (07:00 UTC) runs **all** silver checks (sleep + glucose + workouts +
  recovery + weather + daily + strain + body + fitness + workout-splits + whoop-workouts)
  independently, so a *stopped* silver asset is still caught.

The rebuild schedules default to stopped and are enabled in the UI; `silver_checks_daily`
starts running. Rebuild on demand (e.g. after a bronze backfill) with
`dagster job execute --job silver_sleep_job` (or any family's job).

## Deployment

See [DEPLOYMENT.md → Silver](DEPLOYMENT.md#silver-cross-subject-layer) and
[ENV_TEMPLATE.md](ENV_TEMPLATE.md): bronze mounted **read-only**, `SILVER_ROOT` writable on a
separate volume, and a reserved `SILVER_MONITOR_DIR` (for the forthcoming silver monitor; unused
today).
