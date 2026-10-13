---
parent: silver
prefix: SILVER-DAILY
---

# Silver daily (daily summary, body, fitness)

## Context

Three Garmin tables describe the body day to day, each at its own grain:

- **`silver_daily`**: one row per local day of movement and wellness (from `garmin/user_summary`);
- **`silver_body`**: one row per weigh-in (from `garmin/daily_weigh_ins`);
- **`silver_fitness`**: one row per snapshot day of fitness state (from `garmin/max_metrics`,
  `garmin/training_status` and `garmin/race_predictions`).

Gold's daily wellness mart reads two of them: `silver_daily` joins one-to-one on the day, and
`silver_body`'s latest weigh-in is carried forward. `silver_fitness` feeds the Fitness dashboard. Silver keeps Garmin's units and grain; trends
and conversions are gold's.

## Assets

| Asset | Grain | Reads | Writes |
|---|---|---|---|
| `silver_daily` | One row per `activity_date` | `garmin/user_summary` (bronze dependency `garmin_bronze_user_summary`) | `{SILVER_ROOT}/daily/silver_daily.parquet` |
| `silver_body` | One row per `sample_pk` | `garmin/daily_weigh_ins` (bronze dependency `garmin_bronze_daily_weigh_ins`) | `{SILVER_ROOT}/body/silver_body.parquet` |
| `silver_fitness` | One row per `snapshot_date` | `garmin/max_metrics`, `garmin/training_status`, `garmin/race_predictions` (bronze dependencies `garmin_bronze_max_metrics`, `garmin_bronze_training_status`, `garmin_bronze_race_predictions`) | `{SILVER_ROOT}/fitness/silver_fitness.parquet` |

Each run records `rows`, `bronze_files` and `path` as metadata (for `silver_fitness`,
`bronze_files` counts all three collections).

## Shared rules

- **Garmin re-captures, so each table keeps the latest capture** (the 13-digit fetch time in the
  bronze filename) per key. Captures that tie are picked arbitrarily.
- **Null-safe fields.** Older days predate some sensors; a missing field is `NULL` and the row is
  kept.
- **Integer columns** are cast from the payload's number, so a fractional value is rounded.

## `silver_daily`

`garmin/user_summary` is a flat daily object carrying steps, distance, every calorie type,
floors, intensity minutes, resting/min/max heart rate, the stress breakdown, body battery, SpO2
and respiration. So this one table covers what the per-metric daily collections (`daily_steps`,
`floors`, `intensity_minutes`, `resting_heart_rate`, and the daily values of `stress`, `spo2`,
`respiration`, `body_battery`) also hold; those collections matter only for intraday detail.

- **Key:** `activity_date`, from `calendarDate` (already local). A payload without a parseable
  `calendarDate` is dropped.
- **Stress levels** (`avg_stress_level`, `max_stress_level`) map Garmin's `-1` and `-2` "no data"
  values to `NULL`.
- **Units** as Garmin reports them: metres, kilocalories, seconds, minutes.

| Group | Columns (source key) |
|---|---|
| Movement | `total_steps` (`totalSteps`), `step_goal` (`dailyStepGoal`), `total_distance_m` (`totalDistanceMeters`), `floors_ascended`, `floors_descended` |
| Calories | `total_kilocalories`, `active_kilocalories`, `bmr_kilocalories` |
| Intensity | `moderate_intensity_min`, `vigorous_intensity_min`, `intensity_minutes_goal` |
| Heart | `min_heart_rate`, `max_heart_rate`, `resting_heart_rate`, `avg7d_resting_heart_rate` (`lastSevenDaysAvgRestingHeartRate`) |
| Stress | `avg_stress_level`, `max_stress_level`, `rest_stress_duration`, `low_stress_duration`, `medium_stress_duration`, `high_stress_duration` |
| Body battery | `body_battery_high`, `body_battery_low`, `body_battery_charged`, `body_battery_drained` (`bodyBattery{Highest,Lowest,Charged,Drained}Value`) |
| Vitals | `avg_spo2` (`averageSpo2`), `lowest_spo2`, `avg_waking_respiration`, `highest_respiration`, `lowest_respiration` |
| Time in state | `highly_active_seconds`, `active_seconds`, `sedentary_seconds`, `sleeping_seconds` |

Distances, calories, floors, the seven-day resting heart rate and respiration are `DOUBLE`; the
rest are `INT`.

## `silver_body`

Each `garmin/daily_weigh_ins` payload covers a trailing window: `{startDate, endDate,
dateWeightList: [...], totalAverage}`. Each `dateWeightList` element is one weigh-in, and
overlapping windows repeat it across files. Weigh-ins are sparse, not daily. A payload without
`dateWeightList` contributes nothing; one whose `dateWeightList` is present but isn't an array
**fails the rebuild**, as for every silver table reading an array.

- **Key:** `samplePk`, Garmin's measurement id, dedup to the latest capture. A weigh-in whose
  `samplePk` is missing or JSON null is dropped; one whose `samplePk` isn't numeric is kept with a
  null key, which fails the uniqueness check at ERROR.
- A weigh-in without `timestampGMT` is kept with `measured_ts_utc` null.
- **`measured_date`** is the weigh-in's `calendarDate` (local); informational, since a day can
  rarely hold two weigh-ins. **`measured_ts_utc`** is `timestampGMT` (epoch milliseconds) as a UTC
  timestamp.
- **Units are SI:** masses in **kg** (Garmin's grams ÷ 1000), fat and water in percent, BMI
  unitless. Gold converts to pounds.

| Column | Type | Source |
|---|---|---|
| `sample_pk` | BIGINT | `samplePk`: the key |
| `measured_date` | DATE | `calendarDate` |
| `measured_ts_utc` | TIMESTAMP | `timestampGMT` (epoch ms, UTC) |
| `weight_kg`, `bone_mass_kg`, `muscle_mass_kg`, `weight_delta_kg` | DOUBLE | `weight`, `boneMass`, `muscleMass`, `weightDelta`, each ÷ 1000 |
| `bmi` | DOUBLE | `bmi` |
| `body_fat_pct`, `body_water_pct` | DOUBLE | `bodyFat`, `bodyWater` |
| `physique_rating`, `visceral_fat`, `metabolic_age` | INT | `physiqueRating`, `visceralFat`, `metabolicAge` |
| `source_type` | VARCHAR | `sourceType` |

## `silver_fitness`

Three Garmin endpoints describe fitness state: VO2max (`max_metrics`), training status
(`training_status`) and race predictions (`race_predictions`). Garmin reports each as a current
value, carried until it changes, and the capture re-requests them for many days. Silver turns them
into **one row per day the values apply to** (`snapshot_date`), joining the three on the day: a
day present in any of them is a row, with the others' columns null.

- **VO2max** has a running and a cycling value (`max_metrics[0].generic.vo2MaxValue` and
  `cycling.vo2MaxValue`), **each dated by its own `calendarDate`**. Most captures are empty
  arrays, because Garmin reports VO2max only on run or ride days; only captures carrying a value
  take part, so an empty capture can never erase one. Per sport and date, the latest capture
  wins.
- **Training status** comes from `mostRecentTrainingStatus.latestTrainingStatusData`, a map with
  one entry per device. Each entry is dated by its own `calendarDate`, the day that status took
  effect, so a value repeated in later captures collapses onto that day. **Per date, the entry
  Garmin flags `primaryTrainingDevice` wins, then the latest capture.** For this user that's the
  Edge 1040 bike computer, used only on rides: when it has a status for a day, it's the one to use,
  and the Forerunner 945's entry for the same day is a duplicate. Every other day comes from the
  Forerunner. `training_device_id` records which device supplied the day. A capture whose map is
  empty contributes nothing.
- **Race predictions** have no date of their own that tracks the snapshot (their `calendarDate`
  only moves when a prediction changes), so they're dated by the **bronze `dt` folder**: the
  collection is captured once a day, and the folder is the UTC day of that capture. This is the one
  use of the bronze partition as a date in silver. Per date, the latest capture wins.
- **History** reaches back to 2022 for VO2max and training status, from a bulk historical pull;
  race predictions start with the capture on 2026-06-06 and grow a day at a time.

| Column | Type | Source |
|---|---|---|
| `snapshot_date` | DATE | The day the values apply to: the key |
| `vo2max_running`, `vo2max_cycling` | DOUBLE | `max_metrics[0].generic.vo2MaxValue`, `max_metrics[0].cycling.vo2MaxValue` |
| `training_status_code`, `weekly_training_load` | INT | `trainingStatus`, `weeklyTrainingLoad` |
| `training_status_phrase` | VARCHAR | `trainingStatusFeedbackPhrase` |
| `training_device_id` | BIGINT | The supplying entry's `deviceId` |
| `race_5k_sec`, `race_10k_sec`, `race_half_marathon_sec`, `race_marathon_sec` | INT | `time5K`, `time10K`, `timeHalfMarathon`, `timeMarathon` |

## Asset checks

| Check | Asset | Severity | Passes when |
|---|---|---|---|
| `daily_date_unique_nonnull` | `silver_daily` | ERROR | `activity_date` is unique and never null |
| `daily_value_ranges` | `silver_daily` | ERROR | Every checked non-null value is in range: `total_steps` 0–200,000; `total_distance_m` ≥ 0; `active_kilocalories` 0–30,000; `resting_heart_rate` 20–150; `max_heart_rate` 20–240; `avg_stress_level` 0–100; `avg_spo2` 50–100; `body_battery_high` 0–100 |
| `daily_coverage_vs_bronze` | `silver_daily` | WARN | Silver's row count ≥ bronze's distinct `calendarDate`s; also reports days with steps |
| `silver_daily_freshness` | `silver_daily` | ERROR | The Parquet was written within the last 36 hours |
| `body_sample_unique_nonnull` | `silver_body` | ERROR | `sample_pk` is unique, and neither it nor `measured_date` is null |
| `body_value_ranges` | `silver_body` | ERROR | Every non-null value is in range: weight 20–300 kg, BMI 10–80, body fat 0–70 %, water 0–100 %, muscle 0–200 kg (catches a grams-for-kilograms slip) |
| `body_coverage_vs_bronze` | `silver_body` | WARN | Silver's row count ≥ bronze's distinct numeric `samplePk`s; also reports distinct days |
| `silver_body_freshness` | `silver_body` | ERROR | The Parquet was written within the last 36 hours |
| `fitness_day_unique_nonnull` | `silver_fitness` | ERROR | `snapshot_date` is unique and never null |
| `fitness_value_ranges` | `silver_fitness` | ERROR | VO2max within 10–90; weekly load ≥ 0; 5K and marathon predictions > 0 |
| `fitness_coverage` | `silver_fitness` | WARN | The table has at least one row; reports each metric's coverage (sparse is expected) |
| `silver_fitness_freshness` | `silver_fitness` | ERROR | The Parquet was written within the last 36 hours |

Every check fails at ERROR, and pages, if its table's Parquet doesn't exist.

## Schedule

Each table has its own job, which also runs its checks, and a daily schedule that starts stopped:
`silver_daily_job` at **06:40 UTC** (`silver_daily_daily`), `silver_body_job` at **06:42 UTC**
(`silver_body_daily`) and `silver_fitness_job` at **06:44 UTC** (`silver_fitness_daily`). All
follow Garmin's 05:30 daily capture.

## Decisions & Alternatives

| Decision | Chosen | Alternatives Considered | Rationale |
|---|---|---|---|
| Daily source | `user_summary` alone | One table per per-metric collection | The summary already carries every daily metric; the per-metric collections add only intraday detail. |
| Body grain | One row per weigh-in | One row per day | Weigh-ins are sparse events with their own id; gold carries the latest forward. |
| Body units | Kilograms | Garmin's grams | SI for silver; a grams-for-kg slip is caught by the range check. |
| Fitness dates | VO2max and training status by their own `calendarDate`; race predictions by the bronze `dt` | The bronze `dt` for all three | Those two are captured per requested day and say which day they apply to; dating by folder misdated a bulk historical pull and discarded years of values. Race predictions carry no tracking date. |
| Training status device | Garmin's primary training device first, then latest capture | Latest capture regardless; always the watch | The Edge 1040 is used only on rides and its status is the one to use; the watch's same-day entry is a duplicate. Latest-capture flipped between devices across re-captures. |
| Empty captures | Ignored for VO2max and training status | Let the latest capture win | An empty capture would erase a day's value. |
| Fitness grain | One row per day across three collections | Three tables | They're one daily fitness state; a spine join keeps a day from any collection. |
| Endurance and hill scores | Not carried | Parse nested DTO lists | Their values sit in nested windowed lists with the top-level fields null. |

## References

- `packages/silver/src/grecohome_silver/daily.py`, `body.py`, `fitness.py`, and their
  `dagster/*_assets.py` and `dagster/*_checks.py` modules
- [Garmin capture design](../../garmin/capture/capture-design.md)
