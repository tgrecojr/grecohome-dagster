---
parent: silver
prefix: SILVER-WORKOUTS
---

# Silver workouts

## Context

Three tables describe exercise, from two devices:

- **`silver_workouts`**: one row per Garmin activity (from `garmin/activities`);
- **`silver_workout_splits`**: one row per lap of a Garmin activity (from
  `garmin/activity_splits`), the per-lap detail `silver_workouts` doesn't carry;
- **`silver_whoop_workouts`**: one row per Whoop workout (from `whoop/workout`).

**Garmin and Whoop are kept apart, and neither is authoritative**, as with sleep. They log
different things (the Whoop also records strength work, yard work and walking), they overlap on
runs and rides, and they share no key. So there's no merged workout table and no cross-device
matching in silver; gold compares or picks a device per question.

Per-activity and per-lap facts only. Weekly or monthly training load is analysis, which belongs
in gold, joining these tables to sleep and glucose by local date (`activity_date`,
`workout_date`); a day can hold several workouts, so neither date is unique. Activities are
intermittent, with no daily cadence, so gaps between days are normal and coverage is a WARN.

Since the Whoop arrived (2025-12-18) it carries most of the activity story; Garmin activities
come and go with the user's running and cycling.

## Assets

| Asset | Grain | Reads | Writes |
|---|---|---|---|
| `silver_workouts` | One row per `activity_id` | `garmin/activities` (bronze dependency `garmin_bronze_activities`) | `{SILVER_ROOT}/workouts/silver_workouts.parquet` |
| `silver_workout_splits` | One row per (`activity_id`, `lap_index`) | `garmin/activity_splits` (bronze dependency `garmin_bronze_activity_splits`) | `{SILVER_ROOT}/workout_splits/silver_workout_splits.parquet` |
| `silver_whoop_workouts` | One row per `workout_id` | `whoop/workout` (bronze dependency `whoop_bronze_workout`) | `{SILVER_ROOT}/whoop_workouts/silver_whoop_workouts.parquet` |

Each run records `rows`, `bronze_files` and `path` as metadata.

## Shared rules

- **Arrays.** Each table reads one JSON array per payload (the activities payload itself,
  `lapDTOs`, `records`). A payload where that array is absent contributes no rows. One where it is
  present but isn't an array **fails the rebuild**: bronze holds only successful responses, so
  that means a source format change, and skipping it would rebuild an empty table that every
  check passes. Failing keeps the last good table for the freshness check to catch.
- **Garmin re-captures, so both Garmin tables keep the latest capture** (the 13-digit fetch time
  in the bronze filename): per activity for `silver_workouts`, and per activity's laps for
  `silver_workout_splits`. Captures that tie on fetch time are picked arbitrarily.
- **Whoop rescores, so `silver_whoop_workouts` keeps the latest `updated_at`** for each workout
  (equal values pick arbitrarily). `updated_at` is compared as text, which orders correctly while
  Whoop emits one ISO-8601 UTC format.
- **Unkeyable records.** A record whose key field is missing or JSON null is dropped. A Garmin
  `activityId` or `lapIndex` that is present but not numeric is kept with a null key; such rows
  collapse into one null-key row that fails the table's uniqueness check at ERROR, so a format
  change pages. A record missing the field its date comes from (`startTimeLocal`, `start`) is kept
  with a null date, which fails the same check.
- **Deleted activities stay.** Deduplication runs across every capture in bronze, so an activity
  or workout deleted at the source remains in silver: a later capture that lacks it removes
  nothing. Bronze, the source of truth, still holds it.
- **Null-safe fields.** A missing metric is `NULL` and the row is kept: distance on a non-distance
  sport, calories or elevation on a lap that lacks them, heart rate on a workout that didn't
  record it, `end` on a workout still open.
- **Integer columns** (heart rates, `lap_index`, `sport_id`) are cast from the payload's number,
  so a fractional value is rounded.
- **Timestamps** are naive `TIMESTAMP`s. Garmin's `startTimeLocal` holds local time; every
  `*_gmt` column and Whoop's `start_ts` and `end_ts` hold UTC.
- **No links are enforced.** Silver doesn't check that every lap's activity is in
  `silver_workouts`.

## `silver_workouts` (Garmin activities)

Each `garmin/activities` payload is a top-level JSON **array** of activity objects, empty (`[]`)
on days with none. Overlapping capture windows repeat an activity across files.

- **Key:** `activityId`, dedup to the latest capture. An activity whose `activityId` is missing
  or JSON null is dropped.
- **`activity_date`** is the date of `startTimeLocal` (its first 10 characters). Garmin already
  gives local time, so no offset logic is needed.
- **Heart rate of 0** means the activity recorded no heart rate. Garmin usually sends `0` (and
  sometimes omits the field, giving null), so the range check allows 0.
- **HR zones** (`hr_z1_sec`…`hr_z5_sec`) are present on a subset of activities.

| Column | Type | Source |
|---|---|---|
| `activity_id` | BIGINT | `activityId`: the key |
| `activity_name` | VARCHAR | `activityName` |
| `activity_type` | VARCHAR | `activityType.typeKey` (running, cycling, yoga …) |
| `activity_date` | DATE | Date of `startTimeLocal` |
| `start_time_local`, `start_time_gmt` | TIMESTAMP | `startTimeLocal` (local), `startTimeGMT` (UTC) |
| `duration_sec`, `moving_duration_sec`, `elapsed_duration_sec` | DOUBLE | `duration`, `movingDuration`, `elapsedDuration` (seconds) |
| `distance_m` | DOUBLE | `distance` (metres) |
| `calories` | DOUBLE | `calories` |
| `avg_hr`, `max_hr` | INT | `averageHR`, `maxHR` (0 when not recorded) |
| `hr_z1_sec` … `hr_z5_sec` | DOUBLE | `hrTimeInZone_1` … `hrTimeInZone_5` (seconds) |
| `device_id` | BIGINT | `deviceId` |

## `silver_workout_splits` (Garmin laps)

Each `garmin/activity_splits` payload is one activity: `{activityId, lapDTOs: [...], …}`.
`activityId` is in the payload, so it joins `silver_workouts.activity_id` directly.

- **Key:** (`activity_id`, `lap_index`) from the payload's `activityId` and each lap's
  `lapIndex`. A lap whose `activityId` or `lapIndex` is missing is dropped. `lapIndex` starts at
  1, and the range check fails a lap below 1.
- **An activity's laps all come from its latest capture.** The newest payload for an activity
  that has a `lapDTOs` array supplies every one of its laps. A capture whose `lapDTOs` is missing
  or null is ignored, so it can never wipe an activity's laps; one whose `lapDTOs` is empty (`[]`)
  does count, and leaves the activity with no laps, so an activity re-lapped to fewer laps loses the old ones, and
  laps from different captures never mix. Two laps with the same `lapIndex` in one payload keep
  one of them arbitrarily.
- **HR zones aren't here**; they're per activity in `silver_workouts`.
- **Units:** metres, metres per second, seconds and bpm, as Garmin reports them.

| Column | Type | Source |
|---|---|---|
| `activity_id` | BIGINT | Payload `activityId` |
| `lap_index` | INT | `lapIndex` |
| `lap_start_gmt` | TIMESTAMP | `startTimeGMT` (UTC) |
| `duration_sec`, `moving_duration_sec` | DOUBLE | `duration`, `movingDuration` |
| `distance_m` | DOUBLE | `distance` |
| `avg_speed_mps`, `max_speed_mps` | DOUBLE | `averageSpeed`, `maxSpeed` |
| `avg_hr`, `max_hr` | INT | `averageHR`, `maxHR` |
| `calories` | DOUBLE | `calories` |
| `elevation_gain_m`, `elevation_loss_m` | DOUBLE | `elevationGain`, `elevationLoss` |

## `silver_whoop_workouts`

`whoop/workout` uses Whoop's `{"records": [...]}` envelope; each record has a UUID `id`,
`start` and `end` (UTC), `timezone_offset`, `updated_at`, `sport_name`, `sport_id`,
`score_state` and a nested `score`.

- **Key:** `id` (a UUID, kept as text), dedup to the latest `updated_at`. A record without an `id`
  is dropped.
- **`workout_date`** is the local date of `start`: `start` shifted by the record's own
  `timezone_offset` (`±HH:MM`, the minutes taking the hours' sign), falling back to the UTC date of
  `start` when the offset is missing or unparseable.
- **Metrics are whatever `score` holds.** Some workouts Whoop reports as `SCORED` still carry no
  `score` (about 20 of 266 when profiled), so their metrics are null; `score_state` alone doesn't
  mark them.
- **Units** stay as Whoop reports them: `kilojoules` (gold converts to kcal), metres, and the
  unitless 0–21 strain scale.

| Column | Type | Source |
|---|---|---|
| `workout_id` | VARCHAR | `id`: the key |
| `workout_date` | DATE | Local date of `start` |
| `sport_name` | VARCHAR | `sport_name` |
| `sport_id` | INT | `sport_id` |
| `start_ts`, `end_ts` | TIMESTAMP | `start`, `end` (UTC) |
| `strain` | DOUBLE | `score.strain` (0–21) |
| `avg_heart_rate`, `max_heart_rate` | INT | `score.average_heart_rate`, `score.max_heart_rate` |
| `kilojoules` | DOUBLE | `score.kilojoule` |
| `distance_m` | DOUBLE | `score.distance_meter` |
| `altitude_gain_m` | DOUBLE | `score.altitude_gain_meter` |
| `percent_recorded` | DOUBLE | `score.percent_recorded` |
| `score_state` | VARCHAR | `score_state` |

## Asset checks

| Check | Asset | Severity | Passes when |
|---|---|---|---|
| `workouts_id_unique_nonnull` | `silver_workouts` | ERROR | `activity_id` is unique, and neither it nor `activity_date` is null |
| `workouts_value_ranges` | `silver_workouts` | ERROR | Durations, distance and calories are ≥ 0; `duration_sec` < 7 days; `distance_m` < 1,000 km; `avg_hr` and `max_hr` within 0–240 |
| `workouts_coverage_vs_bronze` | `silver_workouts` | WARN | Silver's row count ≥ bronze's distinct numeric `activityId`s |
| `silver_workouts_freshness` | `silver_workouts` | ERROR | The Parquet was written within the last 36 hours |
| `splits_lap_unique_nonnull` | `silver_workout_splits` | ERROR | (`activity_id`, `lap_index`) is unique and neither is null |
| `splits_value_ranges` | `silver_workout_splits` | ERROR | `duration_sec`, `distance_m` and `avg_speed_mps` are ≥ 0; `avg_hr` and `max_hr` within 0–240; `lap_index` ≥ 1 |
| `splits_coverage_vs_bronze` | `silver_workout_splits` | WARN | Silver's row count ≥ the number of distinct numeric (`activityId`, `lapIndex`) pairs in the captures the table selects (each activity's latest with a `lapDTOs` array); also reports distinct activities |
| `silver_workout_splits_freshness` | `silver_workout_splits` | ERROR | The Parquet was written within the last 36 hours |
| `whoop_workout_unique_nonnull` | `silver_whoop_workouts` | ERROR | `workout_id` is unique, and neither it nor `workout_date` is null |
| `whoop_workout_value_ranges` | `silver_whoop_workouts` | ERROR | `strain` within 0–21; heart rates within 20–240; `kilojoules` and `distance_m` ≥ 0 |
| `whoop_workout_coverage_vs_bronze` | `silver_whoop_workouts` | WARN | Silver's row count ≥ bronze's distinct `id`s; also reports distinct sports |
| `silver_whoop_workouts_freshness` | `silver_whoop_workouts` | ERROR | The Parquet was written within the last 36 hours |

Every check fails at ERROR, and pages, if its table's Parquet doesn't exist. A coverage drop
means dedup or a filter is losing rows.

## Schedule

Each table has its own job, which also runs its checks, and a daily schedule that starts stopped:
`silver_workouts_job` at **06:45 UTC** (`silver_workouts_daily`), `silver_workout_splits_job` at
**06:47 UTC** (`silver_workout_splits_daily`) and `silver_whoop_workouts_job` at **06:53 UTC**
(`silver_whoop_workouts_daily`). All follow the Garmin daily (05:30) and Whoop hourly captures.

## Decisions & Alternatives

| Decision | Chosen | Alternatives Considered | Rationale |
|---|---|---|---|
| Devices | Separate Garmin and Whoop tables | One merged workout table; match overlapping sessions | Independent devices with no shared key and different coverage; merging would invent matches and hide disagreement. |
| Laps | Their own table | Nest laps in `silver_workouts` | The lap grain doesn't fit a per-activity row; gold joins on `activity_id` when it needs them. |
| Lap dedup | All of an activity's laps from its latest capture | Latest capture per (activity, lap) | Per-lap dedup would keep stale laps from older captures after a re-lap and could mix laps from different snapshots. |
| Non-array payloads | Fail the rebuild | Skip the payload | Skipping after a format change rebuilds an empty table that passes every check. |
| Lap index floor | ≥ 1 | ≥ 0 | Garmin numbers laps from 1 (every activity, when profiled); a 0 signals a format change. |
| Garmin dedup | Latest capture per key | First capture | Activities are re-captured; a later capture reflects any edit or re-sync. |
| Deleted activities | Kept in silver | Drop an activity a later capture omits | Captures are day windows, so an absence isn't proof of deletion; keeping matches *Completeness over economy*, and bronze still holds it. |
| Activity date | Date of `startTimeLocal` | Date of `startTimeGMT` | Garmin supplies local time, the day the user would name. |
| Heart rate 0 | Kept; the range allows 0 | Map 0 to null | Faithful to the source; gold can treat 0 as missing. |

## References

- `packages/silver/src/grecohome_silver/workouts.py`, `workout_splits.py`, `whoop_workouts.py`,
  and their `dagster/*_assets.py` and `dagster/*_checks.py` modules
- [Garmin capture design](../../garmin/capture/capture-design.md): activities and splits capture
