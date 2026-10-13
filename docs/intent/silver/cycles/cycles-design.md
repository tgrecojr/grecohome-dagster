---
parent: silver
prefix: SILVER-CYCLES
---

# Silver cycles (recovery and strain)

## Context

Whoop organizes its day around the **physiological cycle**: one cycle runs from one wake to the
next. Two Whoop scores hang off each cycle, and silver projects each into its own table:

- **`silver_recovery`**: how recovered the body is at the start of the cycle (from
  `whoop/recovery`);
- **`silver_strain`**: how much exertion accumulated during the cycle (from `whoop/cycle`).

They share their shape: a `{"records": [...]}` envelope, one record per cycle, a numeric
**`cycle_id`** as the key, and Whoop rescoring records as the day fills in. `cycle_id` is also
the link between them and to sleep (`silver_sleep.whoop_cycle_id`), so gold can line up a night's
sleep, the next morning's recovery and the day's strain. Per-cycle facts only; trends and
correlations are gold.

## Assets

| Asset | Grain | Reads | Writes |
|---|---|---|---|
| `silver_recovery` | One row per `cycle_id` | `whoop/recovery` (bronze dependency `whoop_bronze_recovery`) | `{SILVER_ROOT}/recovery/silver_recovery.parquet` |
| `silver_strain` | One row per `cycle_id` | `whoop/cycle` (bronze dependency `whoop_bronze_cycle`) | `{SILVER_ROOT}/strain/silver_strain.parquet` |

Each run records `rows`, `bronze_files` and `path` as metadata.

## Shared rules

- **Records:** every element of each payload's `records` array is one cycle record. A payload
  without `records` contributes no rows. A `records` that is present but isn't an array **fails
  the rebuild**: bronze holds only successful responses, so that means Whoop changed its format,
  and skipping it would rebuild an empty table that every check passes. Failing leaves the last
  good table in place for the freshness check to catch.
- **Dedup:** one row per `cycle_id`, keeping the latest `updated_at` (equal values pick
  arbitrarily). `updated_at` is compared as text, which orders correctly while Whoop emits one
  ISO-8601 UTC format.
- **Unkeyable records.** A record whose cycle id is missing or JSON null is dropped. One whose id
  isn't numeric is kept with `cycle_id` null; all such records collapse into one null-key row,
  which fails the table's uniqueness check at ERROR, so a change in Whoop's id format pages. A
  record missing the field its date comes from (`created_at`, `start`) is kept with a null date,
  which fails the same check.
- **Dates are informational; `cycle_id` is the key.** A calendar day can hold two cycle
  starts, so neither table is unique per date. Gold joins on `cycle_id`.
- **Timestamps** are naive `TIMESTAMP`s holding UTC; Whoop emits them with a `Z` suffix. (A
  non-zero offset in the string would be dropped by the cast, not applied.)
- **No links are enforced.** Silver doesn't check that a cycle in one table appears in the other or
  in sleep; a missing partner shows up as a null in gold's join.
- **Integer columns** (`avg_heart_rate`, `max_heart_rate`, `step_count`) are cast from the
  payload's number, so a fractional value is rounded.

## `silver_recovery`

Each recovery record carries `cycle_id` and `sleep_id` (both one-to-one with the recovery: when
profiled, every recovery matched exactly one Whoop sleep),
`created_at` and `updated_at`, and a nested `score`.

- **`recovery_date`** is the **UTC date of `created_at`**. Recovery records carry no
  `timezone_offset`, so a local date can't be derived from the record alone. `created_at` falls
  around the wake morning, so the UTC date is a stable approximation of it. Precise local
  alignment comes from joining to the sleep (on `sleep_id` or `cycle_id`), which carries the
  sleep's local night.
- **`user_calibrating`** is Whoop's flag for its calibration days, defaulting to false; it's
  surfaced, not filtered.
- **`score_state`** is carried as Whoop reports it. A recovery Whoop hasn't scored has no `score`,
  so its metrics are null and `user_calibrating` is false; `score_state` is what tells it apart
  from a scored, non-calibrating day.

| Column | Type | Source |
|---|---|---|
| `cycle_id` | BIGINT | `cycle_id`: the key; joins `silver_sleep_whoop.whoop_cycle_id` and `silver_strain.cycle_id` |
| `sleep_id` | VARCHAR | `sleep_id`: joins `silver_sleep_whoop.whoop_sleep_id` |
| `recovery_date` | DATE | UTC date of `created_at` |
| `created_at` | TIMESTAMP | `created_at` (UTC) |
| `recovery_score` | DOUBLE | `score.recovery_score` (%) |
| `resting_heart_rate` | DOUBLE | `score.resting_heart_rate` (bpm) |
| `hrv_rmssd_milli` | DOUBLE | `score.hrv_rmssd_milli` (ms) |
| `spo2_percentage` | DOUBLE | `score.spo2_percentage` (%) |
| `skin_temp_celsius` | DOUBLE | `score.skin_temp_celsius` (°C) |
| `user_calibrating` | BOOLEAN | `score.user_calibrating`, false when absent |
| `score_state` | VARCHAR | `score_state` (`SCORED` once Whoop has scored the recovery) |

## `silver_strain`

Each cycle record carries the cycle `id`, `start` and `end` (UTC), `timezone_offset`,
`score_state`, `step_count` and a nested `score`.

- **`strain_date`** is the **local date of `start`**: a cycle begins at wake, so its start day is
  the day the strain accrued. `start` is shifted by the record's own `timezone_offset` (`±HH:MM`,
  the minutes taking the hours' sign), falling back to the UTC date of `start` when the offset is
  missing or unparseable. An offset without a colon (`+0530`) loses its minutes.
- **The current cycle** has no `end` yet: it's kept, with `end_ts` null and its strain so far.
  Later captures restate it until it closes.
- **Metrics are whatever `score` holds**, regardless of `score_state`. Whoop sends a `score` only
  once a cycle is scored, so an unscored cycle is kept with null metrics; `score_state` is carried
  so gold can tell a scored cycle from one that isn't.
- **`step_count`** is a top-level field Whoop added on 2026-09-23. Earlier captures lack it, so
  it's null before then and never back-filled.
- **Units** stay as Whoop reports them: `kilojoules` (gold converts to kcal) and the unitless
  0–21 strain scale.

| Column | Type | Source |
|---|---|---|
| `cycle_id` | BIGINT | `id`: the key |
| `strain_date` | DATE | Local date of `start` |
| `start_ts`, `end_ts` | TIMESTAMP | `start`, `end` (UTC; `end_ts` null for the current cycle) |
| `day_strain` | DOUBLE | `score.strain` (0–21) |
| `kilojoules` | DOUBLE | `score.kilojoule` |
| `avg_heart_rate`, `max_heart_rate` | INT | `score.average_heart_rate`, `score.max_heart_rate` |
| `score_state` | VARCHAR | `score_state` |
| `step_count` | INT | `step_count` (null before 2026-09-23) |

## Asset checks

| Check | Asset | Severity | Passes when |
|---|---|---|---|
| `recovery_cycle_unique_nonnull` | `silver_recovery` | ERROR | `cycle_id` is unique, and neither it nor `recovery_date` is null |
| `recovery_value_ranges` | `silver_recovery` | ERROR | Every non-null value is in range: score 0–100, resting HR 20–120, HRV 0–500, SpO2 50–100, skin temperature 20–45 |
| `recovery_coverage_vs_bronze` | `silver_recovery` | WARN | Silver's row count ≥ bronze's distinct cycle ids; a drop means dedup or a filter is losing cycles |
| `silver_recovery_freshness` | `silver_recovery` | ERROR | The Parquet was written within the last 36 hours |
| `strain_cycle_unique_nonnull` | `silver_strain` | ERROR | `cycle_id` is unique, and neither it nor `strain_date` is null |
| `strain_value_ranges` | `silver_strain` | ERROR | Every non-null value is in range: strain 0–21, average and max HR 20–240, kilojoules ≥ 0, steps ≥ 0 |
| `strain_coverage_vs_bronze` | `silver_strain` | WARN | Silver's row count ≥ bronze's distinct cycle ids; also reports distinct days |
| `silver_strain_freshness` | `silver_strain` | ERROR | The Parquet was written within the last 36 hours |

Every check fails at ERROR, and pages, if its table's Parquet doesn't exist.

## Schedule

Each table has its own job, which also runs its checks, and a daily schedule that starts stopped:
`silver_recovery_job` at **06:50 UTC** (`silver_recovery_daily`) and `silver_strain_job` at
**06:52 UTC** (`silver_strain_daily`). Both follow Whoop's hourly capture.

## Decisions & Alternatives

| Decision | Chosen | Alternatives Considered | Rationale |
|---|---|---|---|
| Tables | Two tables, one per Whoop score, sharing `cycle_id` | One combined cycle table | Each mirrors one bronze collection; a join in silver would couple their rebuilds and dedup. Gold joins them on `cycle_id`. |
| Key | `cycle_id` | A date | A day can hold two cycles; the cycle is Whoop's unit. |
| Recovery date | UTC date of `created_at` | A local date via a join to sleep | Recovery records have no offset; joining sleep inside silver couples tables. The approximation is labelled, and gold joins for precision. |
| Strain date | Local date of `start` via the record's offset | Local date of `end`; UTC date | A cycle starts at wake, so its start day is the day the strain accrued; `end` is null for the open cycle. |
| Open or unscored cycles | Kept, with null or partial metrics | Dropped until scored | A row's existence is data; later captures restate it. |
| Non-array `records` | Fail the rebuild | Skip the payload | Skipping every payload after a format change rebuilds an empty table that passes uniqueness, coverage (0 ≥ 0) and freshness, so nothing pages. Failing keeps the last good table and freshness pages. |
| `score_state` in both tables | Carried as reported | Only in strain; infer from null metrics | Without it an unscored recovery is indistinguishable from a scored day that isn't calibrating. |

## References

- `packages/silver/src/grecohome_silver/recovery.py`, `strain.py`,
  `dagster/recovery_assets.py`, `recovery_checks.py`, `strain_assets.py`, `strain_checks.py`
- [Silver sleep](../sleep/sleep-design.md): `whoop_cycle_id`, `whoop_sleep_id`
