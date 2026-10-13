---
parent: gold
prefix: GOLD-WELLNESS
---

# Gold daily wellness

## Context

`gold_daily_wellness` is the foundational health mart: **one row per local calendar day**,
joining the silver health tables onto a continuous date spine. It's the table the Daily Wellness
dashboard and later analyses (recovery against load, glucose against sleep) build on.

## Asset

| Asset | Grain | Reads | Writes |
|---|---|---|---|
| `gold_daily_wellness` | One row per `day` | `silver_sleep`, `silver_recovery`, `silver_strain`, `silver_daily`, `silver_workouts`, `silver_whoop_workouts`, `silver_glucose`, `silver_body` (declared as dependencies) | `{GOLD_ROOT}/wellness/daily_wellness.parquet` |

Each run records `rows`, `tir_range` (the time-in-range band) and `path` as metadata.

## The spine

The spine runs from the earliest to the latest day found in sleep (`night_date`), recovery
(`recovery_date`), Garmin workouts (`activity_date`), Whoop workouts (`workout_date`), glucose
(`reading_date`), the daily summary (`activity_date`) and strain (`strain_date`). Every day in between is a row. Body weigh-ins don't
extend the spine; they're carried onto it. A glucose day whose readings are all null still counts
toward the bounds but gets no glucose aggregate.

## Joins

| Source | Join to the day | What the day gets |
|---|---|---|
| `silver_sleep` | `night_date` (one-to-one) | A curated subset: `garmin_sleep_score`, `garmin_total_min`, `garmin_rhr`, `whoop_performance_pct`, `whoop_efficiency_pct` (the full row stays in silver) |
| `silver_recovery` | `recovery_date` | One recovery per day, the latest `created_at` (a UTC date can rarely hold two): `recovery_score`, `resting_heart_rate`, `hrv_rmssd_milli`, `spo2_percentage` |
| `silver_strain` | `cycle_id` of that day's kept recovery | The same physiological cycle's `day_strain`, `strain_kilocalories` (kilojoules ÷ 4.184), `strain_avg_hr`, `strain_max_hr` |
| `silver_daily` | `activity_date` (one-to-one) | `steps`, `active_calories`, `distance_km` (metres ÷ 1000), `floors`, `intensity_minutes` (moderate + vigorous), `avg_stress`, `body_battery_high`, `body_battery_low` |
| `silver_workouts` (Garmin) | `activity_date`, aggregated | `workout_count` (0, not null, on a day without), `workout_total_min`, `workout_distance_km`, `workout_calories` |
| `silver_whoop_workouts` | `workout_date`, aggregated | `whoop_workout_count` (0, not null, on a day without), `whoop_workout_min` (from `start_ts` to `end_ts`), `whoop_workout_strain` (the day's highest workout strain) |
| `silver_glucose` | `reading_date`, aggregated over non-null readings | `glucose_readings`, `glucose_mean`, `glucose_min`, `glucose_max`, `glucose_std` (sample), and time in range |
| `silver_body` | As-of: the latest weigh-in on or before the day, after reducing each `measured_date` to its latest weigh-in by `measured_ts_utc` | `weight_kg`, `weight_lb` (× 2.20462), `body_bmi`, `body_fat_pct`, carried forward until the next weigh-in |

- **Glucose time in range** uses **70–140 mg/dL** inclusive: `glucose_tir_pct`, with
  `glucose_pct_below` (< 70) and `glucose_pct_above` (> 140), each a percentage of the day's
  readings.
- **`intensity_minutes`** is null when either the moderate or the vigorous count is missing.
- **Strain** is the cycle of the day's kept recovery, the cycle that began at the previous night's
  sleep onset. The newest day's cycle is still open, so its strain is partial. On a day with two
  recoveries, the earlier recovery's cycle and its strain don't appear.
- **Weight** carries forward indefinitely: a day long after the last weigh-in still shows it.
- **Provenance flags** mark whether each source has a row for the day, not whether its values are
  present:

  | Flag | True when |
  |---|---|
  | `has_sleep` | `silver_sleep` has a row for the night (its curated columns can still all be null) |
  | `has_recovery` | A recovery was kept for the day |
  | `has_strain` | That recovery's cycle has a strain row |
  | `has_daily` | `silver_daily` has the day |
  | `has_workout` | At least one Garmin activity that day |
  | `has_whoop_workout` | At least one Whoop workout that day |
  | `has_glucose` | At least one non-null glucose reading that day |
  | `has_weight` | Some weigh-in exists on or before the day |

## Asset checks

| Check | Severity | Passes when |
|---|---|---|
| `wellness_day_unique_nonnull` | ERROR | `day` is unique and never null |
| `wellness_value_ranges` | ERROR | Every non-null value is in range: `glucose_tir_pct` 0–100, `glucose_mean` 10–600, `recovery_score` 0–100, `workout_count` ≥ 0, `workout_total_min` ≥ 0, `glucose_readings` > 0, `day_strain` 0–21, `strain_kilocalories` ≥ 0, `steps` 0–200,000, `active_calories` ≥ 0, `avg_stress` 0–100, `weight_lb` 44–660, `body_fat_pct` 0–70 |
| `wellness_coverage` | WARN | At least one day has any source; reports the days and each source's day count |

Every check fails at ERROR, and pages, if the mart's Parquet doesn't exist.

## Schedule

`gold_wellness_job` rebuilds the mart and runs its checks daily at **07:30 UTC**
(`gold_wellness_daily`, which starts stopped), after silver's rebuilds and checks.

## Decisions & Alternatives

| Decision | Chosen | Alternatives Considered | Rationale |
|---|---|---|---|
| Grain | One row per local day over a continuous spine | Only days with data | Gaps as explicit rows make rolling windows and streaks straightforward. |
| Recovery per day | Latest `created_at` | Every recovery | The day grain needs one; the per-sleep link stays in silver. |
| Strain | Joined through the kept recovery's `cycle_id` | Joined on `strain_date` | Keeps a day's exertion next to its recovery, the same cycle. |
| Weight | As-of, carried forward | Only weigh-in days | Weigh-ins are sparse; a daily view needs the current weight. |
| Two weigh-ins in a day | The later by `measured_ts_utc` | Whichever the join picks | "Latest weigh-in" should be deterministic. |
| Workouts | Garmin and Whoop in separate columns | One combined count; Garmin only | The devices log independently and can both record one ride; separate columns keep each faithful and never double-count. Garmin alone missed most activity since the Whoop arrived. |
| Strain gaps | Accept the rare day without a kept recovery | Join strain on its own date | Joining through the recovery keeps a day's strain beside the same cycle's recovery; the miss is a couple of days. |
| Time in range | 70–140 mg/dL | 70–180 (the diabetes band) | The metabolic-health band suits a non-diabetic user. |
| Missing silver table | Typed empty relation; the dimension is null | Fail the mart | One not-yet-built table shouldn't block every other dimension. |

## References

- `packages/gold/src/grecohome_gold/daily_wellness.py`, `dagster/assets.py`, `dagster/checks.py`
