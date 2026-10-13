# Silver layer

The silver layer's intent (layer rules, architecture, storage, check and schedule conventions)
lives in [docs/intent/silver/silver-design.md](intent/silver/silver-design.md), and the shared
helpers in [docs/intent/core/silver/silver-design.md](intent/core/silver/silver-design.md).
Sleep, glucose and cycles (recovery, strain) are specified under
[docs/intent/silver/](intent/silver/silver-design.md).

This page still describes the table families not yet specified there:

- **Workouts** — `silver_workouts` (per-activity Garmin). See
  [Workouts (Garmin activities)](#workouts-garmin-activities) below.
- **Workout splits** — `silver_workout_splits` (per-lap Garmin detail; enriches workouts). See
  [Workout splits (Garmin laps)](#workout-splits-garmin-laps) below.
- **Whoop workouts** — `silver_whoop_workouts` (per-workout Whoop activities; the device's
  own activity log, parallel to Garmin's). See [Whoop workouts](#whoop-workouts) below.
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
{SILVER_ROOT}/workouts/silver_workouts.parquet # workouts: one row per Garmin activity
{SILVER_ROOT}/workout_splits/silver_workout_splits.parquet # one row per Garmin lap
{SILVER_ROOT}/whoop_workouts/silver_whoop_workouts.parquet # one row per Whoop workout
{SILVER_ROOT}/weather/silver_weather.parquet # weather: one row per USCRN hourly observation
{SILVER_ROOT}/daily/silver_daily.parquet     # daily summary: one row per local day (Garmin)
{SILVER_ROOT}/body/silver_body.parquet       # body: one row per Garmin weigh-in
{SILVER_ROOT}/fitness/silver_fitness.parquet # fitness: one row per Garmin snapshot day
```

# Workouts (Garmin activities)

`silver_workouts` is a single-source table — one row per Garmin activity.

### Source & event date
Garmin activities bronze (`garmin/activities`) is a top-level JSON **array** of activity objects
per file (empty `[]` on days with none). Garmin appends and re-fetches overlapping windows, so the
same activity recurs across files — dedup by **`activityId`** (keeping the latest fetch). Activities
carry a **local** `startTimeLocal` and a UTC `startTimeGMT`, so the event date is just the local
date — no timezone subtlety.

### Schema (`silver_workouts`)
One row per activity.

| Column | Type | Note |
|---|---|---|
| `activity_id` | BIGINT | dedup key |
| `activity_name` | VARCHAR | |
| `activity_type` | VARCHAR | `activityType.typeKey` (running / cycling / yoga / …) |
| `activity_date` | DATE | local date of `startTimeLocal` |
| `start_time_local` / `start_time_gmt` | TIMESTAMP | local / UTC start |
| `duration_sec` / `moving_duration_sec` / `elapsed_duration_sec` | DOUBLE | seconds |
| `distance_m` | DOUBLE | metres (null on non-distance sports) |
| `calories` | DOUBLE | |
| `avg_hr` / `max_hr` | INT | bpm (`avg_hr` = 0 when HR not recorded) |
| `hr_z1_sec` … `hr_z5_sec` | DOUBLE | seconds in each HR zone (present on a subset) |
| `device_id` | BIGINT | |

### Asset checks
| Check | Severity | What |
|---|---|---|
| `workouts_id_unique_nonnull` | ERROR | one row per `activity_id`; `activity_id`/`activity_date` non-null |
| `workouts_value_ranges` | ERROR | durations/distance/calories ≥ 0 and bounded; HR 0–240 (0 = not recorded) |
| `workouts_coverage_vs_bronze` | WARN | silver activities ≈ bronze distinct `activityId` (no silent drop) |

# Workout splits (Garmin laps)

`silver_workout_splits` is the per-lap breakdown `silver_workouts` doesn't have — one row
per **lap** (`activity_id` + `lap_index`), enriching each workout with its splits.

### Source & event date
`garmin/activity_splits` is `{activityId, lapDTOs: […], eventDTOs}` per activity; `lapDTOs`
is the array of laps. `activityId` is in the **payload** (joins `silver_workouts.activity_id`),
so no sidecar is needed. Garmin re-pulls an activity into several files → dedup by
**(activity_id, lap_index)** keeping the latest fetch. Profiled live: 727 activities, ~4,255
deduped laps. Lap richness varies by activity type (distance / speed / HR near-universal;
calories and elevation on a subset); a missing field is NULL, the lap is kept. HR *zones* are
**not** here — they already live in `silver_workouts` (`hr_z1_sec` …).

### Schema (`silver_workout_splits`)
One row per `(activity_id, lap_index)`: `activity_id` (BIGINT), `lap_index` (INT),
`lap_start_gmt` (TIMESTAMP), `duration_sec`, `moving_duration_sec` (DOUBLE), `distance_m`,
`avg_speed_mps`, `max_speed_mps` (DOUBLE), `avg_hr`, `max_hr` (INT), `calories`,
`elevation_gain_m`, `elevation_loss_m` (DOUBLE).

### Asset checks
| Check | Severity | What |
|---|---|---|
| `splits_lap_unique_nonnull` | ERROR | one row per `(activity_id, lap_index)`; both keys non-null |
| `splits_value_ranges` | ERROR | durations/distance/speed ≥ 0, HR 0–240, lap_index ≥ 0 |
| `splits_coverage_vs_bronze` | WARN | silver laps ≈ bronze distinct laps; reports distinct activities |

# Whoop workouts

`silver_whoop_workouts` is Whoop's own activity log — one row per workout — kept **separate
from `silver_workouts`** (Garmin) on purpose: two devices, neither authoritative, never
blended (the sleep philosophy). It's where the activity story lives once Garmin's tapers off:
since the Whoop arrived (2025-12-18) it logs strength, yard-work, walking, etc. (plus running/
cycling that overlaps Garmin).

### Source & event date
`whoop/workout` is the same `{"records": [...]}` envelope as recovery/strain; each record has
a UUID `id`, `start`/`end` (UTC), `timezone_offset`, `created_at`/`updated_at` (Whoop
rescores), `sport_name`/`sport_id`, `score_state`, and a nested `score` (`strain` 0–21, avg/max
HR, `kilojoule`, `distance_meter`, `altitude_gain_meter`, `percent_recorded`). Dedup by `id`
keeping the latest `updated_at`. `workout_date` is the **local date of `start`** (via the
record's `timezone_offset`, the sleep/strain idiom).

### Typing & units
Canonical to the source: `kilojoules` as Whoop reports it (gold/dashboard → kcal), distance in
metres, strain unitless 0–21. Non-GPS sports (lifting, yard-work) have null distance — kept.

### Schema (`silver_whoop_workouts`)
One row per workout: `workout_id` (VARCHAR, key), `workout_date` (DATE), `sport_name` (VARCHAR),
`sport_id` (INT), `start_ts`/`end_ts` (TIMESTAMP), `strain` (DOUBLE), `avg_heart_rate`/
`max_heart_rate` (INT), `kilojoules`, `distance_m`, `altitude_gain_m`, `percent_recorded`
(DOUBLE), `score_state` (VARCHAR).

### Asset checks
| Check | Severity | What |
|---|---|---|
| `whoop_workout_unique_nonnull` | ERROR | one row per `workout_id`; `workout_id`/`workout_date` non-null |
| `whoop_workout_value_ranges` | ERROR | strain 0–21, HR 20–240, kilojoules/distance ≥ 0 |
| `whoop_workout_coverage_vs_bronze` | WARN | silver workouts ≈ bronze distinct `id`; reports distinct sports |

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
- `silver_workouts_daily` (06:45 UTC) rebuilds `silver_workouts` (after the Garmin daily capture).
- `silver_weather_daily` (06:55 UTC) rebuilds `silver_weather` (USCRN captured a few times a day;
  a daily rebuild keeps silver a current projection).
- `silver_daily_daily` (06:40 UTC) rebuilds `silver_daily` (after the Garmin daily capture).
- `silver_body_daily` (06:42 UTC) rebuilds `silver_body` (Garmin weigh-ins).
- `silver_fitness_daily` (06:44 UTC) rebuilds `silver_fitness` (Garmin snapshot collections).
- `silver_workout_splits_daily` (06:47 UTC) rebuilds `silver_workout_splits` (Garmin laps).
- `silver_whoop_workouts_daily` (06:53 UTC) rebuilds `silver_whoop_workouts` (Whoop activities).
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
