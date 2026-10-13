---
parent: silver
prefix: SILVER-SLEEP
---

# Silver sleep

## Context and the source decision

Two bronze sleep sources exist, with different depth: **Garmin** (`garmin/sleep`, a flat
`dailySleepDTO` per night, history since 2022) and **Whoop** (`whoop/sleep`, `records[]`, since
the Whoop was acquired on 2025-12-18). The user wears both on most recent nights.

**Both are kept side by side, and neither is authoritative.** No wearable observes sleep
directly; each is an independent estimate. Blending them would launder two methods into one
falsely authoritative number and discard their disagreement, which is itself signal. So
`silver_sleep` holds both sources' columns, both nullable, joined by night with a **FULL OUTER
JOIN**. Nothing is coalesced and no "primary" or "best" value is synthesized. Gold picks a device
per question, or compares them.

Because nothing is coalesced, there's no hidden device-switch artifact: a `garmin_*` column is
Garmin's method end to end, and a `whoop_*` column is Whoop's. The one structural gap, `whoop_*`
being null before the device existed, is made explicit by `has_whoop`.

## Assets

```
garmin_bronze_sleep ─▶ silver_sleep_garmin ─┐
                                            ├─▶ silver_sleep   (FULL OUTER JOIN on night_date)
whoop_bronze_sleep  ─▶ silver_sleep_whoop  ─┘
```

| Asset | Grain | Writes |
|---|---|---|
| `silver_sleep_garmin` | One row per night | `{SILVER_ROOT}/sleep/_garmin.parquet` |
| `silver_sleep_whoop` | One row per Whoop sleep id (naps included and flagged) | `{SILVER_ROOT}/sleep/_whoop.parquet` |
| `silver_sleep` | One row per night | `{SILVER_ROOT}/sleep/silver_sleep.parquet` |

The two source assets declare their bronze assets as dependencies (`garmin_bronze_sleep`,
`whoop_bronze_sleep`); `silver_sleep` depends on both source assets and reads their Parquet.
Each run records `rows` and `path` (and `bronze_files` for the source assets) as metadata.

`silver_sleep` reads whatever `_garmin.parquet` and `_whoop.parquet` are on disk. In the job, a
failed source asset skips `silver_sleep`, so the previous unified table stands. Materialized on
its own, it joins the current intermediates, and fails if one doesn't exist.

## The night

- **Garmin:** `dailySleepDTO.calendarDate`, which is already local and is the **wake (morning)
  date**: a sleep from 23:00 to 07:00 is labelled with the morning's date.
- **Whoop:** the **local wake date**, the local date of `end`. `end` is UTC, and each record
  carries a `timezone_offset` (`±HH:MM`), so the night is the date of `end` shifted by that offset.
  The offset's minutes take the hours' sign (`-04:30` is −4 h −30 min). If the offset is missing
  or unparseable, the UTC date of `end` is used, so a night is never dropped.

The offset is the record's own fixed offset, so a sleep spanning a daylight-saving change uses
the offset Whoop recorded for it; there's no IANA-zone logic. An offset that isn't `±HH:MM`
(`Z`, or `-0500` without a colon) falls back or loses its minutes, which only moves the night for
a wake within that many minutes of midnight.

Keying Whoop on the wake date matches Garmin's convention and keeps each sleep a distinct night.
Keying on the bedtime (`start`) date misaligned with Garmin and wrongly merged nights.

**Bad payload shapes.** A Garmin payload that isn't an object with `dailySleepDTO.calendarDate`
(an array, `{}`, `null`) yields no night and is dropped. A Whoop payload without `records`
contributes no rows; one whose `records` isn't an array fails the rebuild.

## Deduplication

- **Garmin:** one row per `night_date`, keeping the **latest capture** (the 13-digit fetch time
  in the bronze filename). Garmin re-captures each recent day daily and later captures can be
  more complete. Payloads without a parseable `calendarDate` are dropped.
- **Whoop:** one row per sleep `id`, keeping the **latest `updated_at`**, because Whoop rescores
  sleeps. `updated_at` is compared as text, which orders correctly while Whoop emits one ISO-8601
  UTC format. Records without an `id`, or whose night can't be derived, are dropped.
- **For the unified table**, the Whoop side is reduced to **one non-nap record per night**
  (latest `start`, a missing `start` sorting last), so the join stays one-to-one per night. A
  second main sleep on the same night stays in `silver_sleep_whoop` but not in `silver_sleep`.
- Captures that tie (the same Garmin fetch time, the same Whoop `id` and `updated_at`) keep an
  arbitrary one of the tied rows; they're the same record.

## Naps

Naps are real data, so `silver_sleep_whoop` keeps them, flagged `is_nap` (from the record's
`nap`, defaulting to false). The unified `silver_sleep` uses only non-nap records, so a night with
only naps has `has_whoop = false`. Garmin's nightly record has no nap concept; if Garmin records
naps separately, they aren't read.

## Typing and units

- **Stage durations in minutes for both sources** (Garmin `*Seconds / 60`, Whoop `*_milli /
  60000`), so `garmin_*_min` and `whoop_*_min` compare directly.
- Dates are `DATE`. Start and end are naive `TIMESTAMP`s holding **UTC**, kept so later questions
  like "what time did I fall asleep" can apply a local offset. Garmin's GMT timestamps may be
  epoch milliseconds or ISO strings; both are parsed, and anything else becomes `NULL`.
- `INT` columns are cast from the payload's number, so a fractional value is rounded.
- Every field is extracted null-safely. Older Garmin nights have no overall score: the night is
  kept with a `NULL` score, never dropped.
- **Not carried:** Garmin's respiration and SpO2 low/high, and Whoop's sleep-need breakdown. They
  stay in bronze and can be added when an analysis needs them.

## Schema (`silver_sleep`)

| Column | Type | Source |
|---|---|---|
| `night_date` | DATE | The night (join key) |
| `garmin_sleep_score` | INT | `dailySleepDTO.sleepScores.overall.value` |
| `garmin_total_min`, `garmin_deep_min`, `garmin_light_min`, `garmin_rem_min`, `garmin_awake_min` | DOUBLE | `sleepTimeSeconds`, `deepSleepSeconds`, `lightSleepSeconds`, `remSleepSeconds`, `awakeSleepSeconds`, each ÷ 60 |
| `garmin_avg_stress` | DOUBLE | `avgSleepStress` |
| `garmin_resp_avg` | DOUBLE | `averageRespirationValue` |
| `garmin_spo2_avg` | DOUBLE | `averageSpO2Value` |
| `garmin_rhr` | INT | Top-level `restingHeartRate` (a sibling of `dailySleepDTO`) |
| `garmin_start_gmt`, `garmin_end_gmt` | TIMESTAMP | `sleepStartTimestampGMT`, `sleepEndTimestampGMT` |
| `whoop_performance_pct`, `whoop_efficiency_pct`, `whoop_consistency_pct` | DOUBLE | `score.sleep_{performance,efficiency,consistency}_percentage` |
| `whoop_resp_rate` | DOUBLE | `score.respiratory_rate` |
| `whoop_deep_min`, `whoop_rem_min`, `whoop_light_min`, `whoop_awake_min` | DOUBLE | `score.stage_summary.total_{slow_wave_sleep,rem_sleep,light_sleep,awake}_time_milli` ÷ 60000 |
| `whoop_disturbances` | INT | `score.stage_summary.disturbance_count` |
| `whoop_cycle_id` | BIGINT | `cycle_id`, the link to recovery and strain |
| `whoop_start`, `whoop_end` | TIMESTAMP | `start`, `end` |
| `has_garmin`, `has_whoop` | BOOLEAN | Whether each source has the night |

`silver_sleep_garmin` holds `night_date` and the `garmin_*` columns. `silver_sleep_whoop` holds
`night_date`, `whoop_sleep_id`, `is_nap` and the `whoop_*` columns.

## Asset checks

| Check | Asset | Severity | Passes when |
|---|---|---|---|
| `garmin_night_unique_nonnull` | `silver_sleep_garmin` | ERROR | `night_date` is unique and never null |
| `whoop_id_unique_night_nonnull` | `silver_sleep_whoop` | ERROR | `whoop_sleep_id` is unique and `night_date` is never null |
| `sleep_night_unique_nonnull` | `silver_sleep` | ERROR | `night_date` is unique and never null |
| `sleep_value_ranges` | `silver_sleep` | ERROR | The Garmin score and Whoop percentages are within 0–100, and every stage-minute column is ≥ 0 and < 1440 |
| `sleep_join_sanity` | `silver_sleep` | WARN | No row lacks both sources, a guard against a join regression. It also reports nights on or after 2025-12-18 that have only one source, as a soft flag for a sync or wear gap |
| `sleep_coverage_split` | `silver_sleep` | WARN | Always passes; reports both / Garmin-only / Whoop-only night counts, so a sudden shift (recent nights going single-source) is visible as a capture problem |
| `garmin_coverage_vs_bronze` | `silver_sleep_garmin` | WARN | Silver's night count ≥ bronze's distinct parseable `calendarDate` count; a drop means dedup or a filter is losing nights |
| `silver_sleep_garmin_freshness`, `silver_sleep_whoop_freshness`, `silver_sleep_freshness` | each table | ERROR | The table's Parquet was written within the last 36 hours (the layer's freshness rule). It measures rebuilds, not data: a night without a device still rebuilds the table |

Every check fails at ERROR, and so pages, if its table's Parquet doesn't exist yet.

## Schedule

`silver_sleep_job` rebuilds all three assets daily at **06:00 UTC** (`silver_sleep_daily`,
which starts stopped), after Garmin's daily capture and Whoop's hourly capture. `silver_sleep`
runs after its two sources, since it depends on them. The job also runs the sleep checks.

## Decisions & Alternatives

| Decision | Chosen | Alternatives Considered | Rationale |
|---|---|---|---|
| Two sources | Side by side, FULL OUTER JOIN, nothing coalesced | Pick a primary device; blend into one value | Each device is an independent estimate; their disagreement is signal, and a coalesced column hides a device-switch artifact. |
| Whoop night | Local wake date (`end` + offset) | Bedtime date (`start`); UTC date of `end` | Matches Garmin's wake-date `calendarDate` (aligned on every overlapping night) and keeps sleeps distinct; bedtime keying misaligned and merged nights. |
| Naps | Kept and flagged in `silver_sleep_whoop`, excluded from the unified row | Drop naps | They're real data; the unified table's grain is one main sleep per night. |
| Units | Minutes for both sources | Keep native units | Direct comparability between `garmin_*_min` and `whoop_*_min`. |
| Three assets | Two typed source assets plus one join | One asset doing everything | Each source's typing and dedup is checkable on its own, and gold can use a source table directly. |

## References

- `packages/silver/src/grecohome_silver/sleep.py`, `dagster/assets.py`, `dagster/checks.py`
