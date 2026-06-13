# Silver / Gold roadmap

Where the derived layers stand today and what's left to build, mapped against **every**
bronze collection actually captured on the lake (profiled 2026-06-13: ~74 collections).

The guiding principle stays: each silver table is a **single-source reduction** of the
sleep template (typed, deduped, one row per logical record, Parquet + asset checks);
gold joins/rolls them up to a useful grain. Bronze is the only source of truth.

## Current state

**Silver (5):** `silver_sleep` (garmin+whoop), `silver_glucose` (lingo), `silver_workouts`
(garmin activities), `silver_recovery` (whoop), `silver_weather` (uscrn).
**Gold (2):** `gold_daily_wellness`, `gold_daily_weather`.

Silver projects **5** logical tables out of ~74 captured collections — most of the bronze
capture is not yet reduced.

## The key leverage point

**`garmin/user_summary` is a daily super-object.** A single record already carries steps,
distance, every calorie type, floors, intensity minutes, resting/min/max HR, the full
stress breakdown, body-battery, SpO2, and respiration — all per local day. So one
`silver_daily` table sourced from `user_summary` **subsumes ~8 separate per-metric
collections** (`daily_steps`, `floors`, `intensity_minutes`, `resting_heart_rate`,
`stress`(daily), `body_battery`(daily), `spo2`(daily), `respiration`(daily)). Those
standalone collections are then only needed for *intraday* detail — deferrable.

## Proposed build, by phase

### Phase 1 — Complete the daily wellness picture (high value, low effort)
Each mirrors the glucose/weather template; all three feed the existing day spine in
`gold_daily_wellness`.

| New silver table | Source collection(s) | Grain / key fields |
|---|---|---|
| **`silver_strain`** | `whoop/cycle` | per cycle (day = local date of `end`): `score.strain`, `kilojoule`→kcal, `average_heart_rate`, `max_heart_rate`. Twin of `silver_recovery`; `whoop_cycle_id` already links from sleep. |
| **`silver_daily`** | `garmin/user_summary` | one row per local day (`calendarDate`): steps, distance, active/total/BMR calories, floors, moderate/vigorous intensity minutes, resting/min/max HR, stress avg/max + durations, body-battery high/low/charged/drained, SpO2 avg/low, respiration avg/high/low, active/sedentary/sleeping seconds. |
| **`silver_body`** | `garmin/daily_weigh_ins` (+ `body_composition`, `weigh_ins`) | one row per weigh-in (sparse, not daily): weight, BMI, body-fat %, body-water %, muscle/bone mass, metabolic age. Restores the deleted Weight dashboard. |

**Gold:** extend `gold_daily_wellness` with strain, daily activity (steps/calories/stress/
body-battery), and latest weight. **Dashboard:** add a Strain↔Recovery panel + steps/
activity; (re)build a Body/Weight dashboard.

### Phase 2 — Readiness & fitness
| New silver table | Source collection(s) | Grain / key fields |
|---|---|---|
| **`silver_hrv`** | `garmin/hrv` | per night: overnight HRV weekly-avg, last-night-avg, baseline low/balanced/upper, status. Companion to recovery. |
| **`silver_fitness`** | `garmin/{training_readiness, training_status, max_metrics, endurance_score, hill_score, race_predictions, fitness_age, lactate_threshold, cycling_ftp, running_tolerance}` | one wide per-day table — VO2max (run/cycle), training readiness score, training status/load, endurance & hill scores, race-time predictions, fitness age. Consolidates many small daily collections. |

**Gold:** new `gold_fitness` mart (VO2max trend, readiness, training load). **Dashboard:**
Fitness / Training.

### Phase 3 — Depth & nutrition
| New silver table | Source collection(s) | Notes |
|---|---|---|
| **`silver_workout_details`** | `garmin/activity_{details,splits,typed_splits,split_summaries,hr_zones,power_zones,summary,weather,exercise_sets,gear}` | per-activity detail (splits, HR/power zones, weather) keyed to `silver_workouts.activity_id`. Higher effort (nested). |
| **`silver_nutrition`** | `garmin/{nutrition_food_log, nutrition_meals, hydration}` | diet + hydration per day; natural pair with `silver_glucose`. |
| **`silver_workouts` (2nd source)** | `whoop/workout` | optionally fold Whoop's own activity scoring in as a second source (device-agreement, like sleep), or a standalone `silver_whoop_workouts`. |
| optional | `garmin/{blood_pressure, menstrual_calendar}` | build only if used. |

### Phase 4 — Intraday & housekeeping (only on demand)
- **Intraday silver** (`heart_rates`, `steps_intraday`, `stress`, `spo2`, `respiration`,
  `body_battery_events`, `daily_events`) — high-resolution time-series; heavy, build only
  when a chart needs sub-daily resolution. The daily values already live in `silver_daily`.
- **Weekly rollups** (`weekly_steps`, `weekly_stress`, `weekly_intensity_minutes`) — derive
  in **gold** from `silver_daily`; do not ingest the weekly bronze collections.
- **Cleanup:** investigate/retire the stale **`uscrn/pa_avondale_2_n/`** bronze path (42
  files) that sits alongside the live `uscrn/hourly/` (a pre-rename remnant). Bronze is the
  source of truth — verify before touching.

## Not silver-worthy (reference / config — skip)
Slowly-changing config and reference, not time-series analytics: `garmin/{devices,
device_settings, device_solar, device_last_used, primary_device, user_settings,
userprofile_settings, activity_types, goals, training_plans, workouts(templates),
available_badges, earned_badges, in_progress_badges, personal_records}`, `whoop/{profile,
body_measurement}` (static height/weight — may seed `silver_body` once).

## Sequencing summary

1. **Phase 1** — `silver_strain`, `silver_daily`, `silver_body` → enrich
   `gold_daily_wellness` + dashboard. *Biggest value per unit effort; `silver_daily` alone
   unlocks ~8 collections.*
2. **Phase 2** — `silver_hrv`, `silver_fitness` → `gold_fitness` + Fitness dashboard.
3. **Phase 3** — `silver_workout_details`, `silver_nutrition`, Whoop workouts.
4. **Phase 4** — intraday (on demand), weekly-in-gold, stale-path cleanup.

Recommended first PR: **`silver_strain` + `silver_daily`** (both pure single-source
reductions that immediately enrich the wellness mart), with `silver_body` close behind.
