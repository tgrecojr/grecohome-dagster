# Silver layer

The silver layer is the typed, deduplicated, analysis-ready projection of bronze. It reads the
raw immutable bronze payloads, extracts the **true event date**, unnests and types fields,
deduplicates to **one row per logical record**, and writes columnar **Parquet**. Silver is
*derived and rebuildable* — it can always be dropped and regenerated from bronze, so it is not
immutable and not precious. **Bronze remains the only source of truth**; silver never touches it.

Tables today:
- **Sleep** — `silver_sleep` (unified daily sleep) + its two source intermediates. The
  pattern-setter: event-date extraction, dedup, typing, Parquet, a two-source join, and asset
  checks. See [Sleep](#sleep) below.
- **Glucose** — `silver_glucose` (per-reading Lingo CGM). The first single-source reduction of
  the template. See [Glucose (Lingo CGM)](#glucose-lingo-cgm) below.
- **Workouts** — `silver_workouts` (per-activity Garmin). See
  [Workouts (Garmin activities)](#workouts-garmin-activities) below.

Later tables (recovery, fitness) are further single-source reductions.

## Invariants

- **Rebuildable, not precious.** Every materialization **fully overwrites** its output (last run
  wins). Silver is a pure projection of *current* bronze — no append/merge, no history of its own.
- **Outside bronze.** Silver writes only under `SILVER_ROOT`; the atomic writer **refuses** any
  path inside `BRONZE_ROOT`. Bronze is mounted read-only in the silver container.
- **Swappable root.** `SILVER_ROOT` is passed by config (mirrors the bronze convention), keeping
  an object-store migration open.
- **Sidecars excluded.** Every bronze read skips `*.meta.json` (the helper excludes them in
  Python before DuckDB ever sees a file), so meta keys can't contaminate a parsed payload.
- **`dt` ≠ event date.** The night is always derived from the payload (`calendarDate` / Whoop
  `start`), never the partition folder.
- **Off the API pools.** Silver makes no source calls; assets and checks carry no `*_api`
  concurrency pool.

## Architecture

```
garmin_bronze_sleep ─▶ silver_sleep_garmin ─┐
                                            ├─▶ silver_sleep   (FULL OUTER JOIN on the night)
whoop_bronze_sleep  ─▶ silver_sleep_whoop  ─┘
```

- Generic, source-agnostic helpers live in **`grecohome_core.silver`** (DuckDB connection,
  sidecar-safe payload reading, the `row_number() … = 1` dedup idiom, atomic Parquet write). The
  sleep-specific **column mapping** lives in `grecohome_silver.sleep` — the one place payload
  fields are named.
- Sleep spans two subjects, so it lives in a **cross-subject `silver` code location** (its own
  image), not inside `whoop` or `garmin`.
- Bronze upstreams are declared by **`AssetKey`** for cross-code-location lineage; the reads are
  **filesystem reads** of `BRONZE_ROOT` via DuckDB, not gRPC calls into the subject locations. So
  the silver image depends only on `grecohome-core` + `duckdb`.

## Storage & format

- **Format: Parquet** (zstd), written via DuckDB `COPY (...) TO ... (FORMAT parquet)`.
- **Layout** under `SILVER_ROOT`:

```
{SILVER_ROOT}/sleep/silver_sleep.parquet     # sleep: the product (one row per night)
{SILVER_ROOT}/sleep/_garmin.parquet          # sleep source intermediate (one row per night)
{SILVER_ROOT}/sleep/_whoop.parquet           # sleep source intermediate (one row per sleep id)
{SILVER_ROOT}/glucose/silver_glucose.parquet # glucose: one row per CGM reading
{SILVER_ROOT}/workouts/silver_workouts.parquet # workouts: one row per Garmin activity
```

- **Atomic overwrite.** Each asset `COPY`s to a temp file in the destination dir and `os.replace`s
  it into place, so a crashed run never leaves a half-written Parquet.
- **Partitioning: none (v1).** The data is small (thousands of nights, ~55k glucose readings); each
  asset is a whole-table rebuild reading all bronze partitions. Partition by year later only if it
  grows.

# Sleep

## The source decision (two co-equal sources)

Two bronze sleep sources exist with different depth: **Garmin** (flat `dailySleepDTO`, ~4 years)
and **Whoop** (`records[]`, since the device was acquired ~2025-12-18). Silver keeps **both
sources' columns side by side, both nullable**, joined by night via a **FULL OUTER JOIN** —
**neither is authoritative**, nothing is coalesced, and no "primary/best" column is synthesized.

No wearable measures sleep with full accuracy; each is an independent *estimate* of a night you
cannot directly observe. Blending them launders two methodologies into a falsely-authoritative
number and discards the disagreement between them — and that disagreement is itself signal. The
user wears both devices on most recent nights and wants both retained. Gold-layer analysis later
chooses a device per question, or compares the two; silver's job is only to faithfully hold both.
See [ADR 0007](adr/0007-silver-sleep.md).

Because nothing is coalesced there is **no cross-device discontinuity**: a `garmin_*` column is
Garmin's methodology end to end, a `whoop_*` column always Whoop's. The only gap is the obvious
one — `whoop_*` is null before the device existed — which `has_whoop` makes explicit.

## Transform rules

### Event date (the night)
- **Garmin:** `dailySleepDTO.calendarDate` (a clean DATE; authoritative, already local).
  This is the **wake/morning date** — a sleep from 11pm to 7am is labelled the morning's
  date, verified against live bronze.
- **Whoop:** the **local wake date** — the local date of `end`. `end` is UTC and the
  record carries a `timezone_offset`, so the night is `CAST(end + timezone_offset AS
  DATE)`. This matches Garmin's wake-date `calendarDate` convention (38/38 alignment in
  the overlap window) and keeps every sleep a distinct night; keying on the bedtime
  (`start`) date instead misaligns with Garmin and falsely merges nights. The offset's
  minutes inherit the hours' sign (`-04:30` → −4h −30m); a missing/unparseable offset
  falls back to the UTC date of `end`, so the night is never dropped.

### Deduplication (bronze is heavily re-captured)
- **Garmin:** dedup key = `calendarDate`; keep the **latest fetch**. The same night appears in
  many files (re-pulls); tie-break by the 13-digit `fetched_ms` in the bronze filename.
- **Whoop:** dedup key = `id` (the sleep UUID); keep the row with the **latest `updated_at`**
  (Whoop rescores). For the unified night, the Whoop side is then collapsed to one **non-nap**
  record per night.

Both use `row_number() OVER (PARTITION BY <key> ORDER BY <recency> DESC) = 1`.

### Naps (Whoop-specific)
Naps are **kept in `silver_sleep_whoop`** and flagged (`is_nap`); the unified per-night
`silver_sleep` uses only `nap = false` records. Naps are real data — not silently dropped from
the source asset, just excluded from the one-row-per-night unified row.

### Typing & units
- **Stage durations normalized to minutes** for both sources (Garmin `*Seconds / 60`, Whoop
  `*_milli / 60000`) so `garmin_*_min` and `whoop_*_min` are directly comparable.
- Dates as DATE, `start`/`end` as TIMESTAMP. Garmin GMT timestamps are epoch-millis-or-ISO
  (parsed null-safe).
- Null-safe: payloads are read as raw JSON and extracted by **JSON path**, so a missing/renamed
  key yields `NULL` rather than an error. Older Garmin nights lack an overall score — the night is
  kept, the score nulled, never dropped.

### Unified join (`silver_sleep`)
FULL OUTER JOIN of the two deduped source assets on `night_date` — one row per night. Both column
sets present and nullable; per-night provenance `has_garmin` / `has_whoop` makes every null
explainable and lets gold compute device deltas / agreement later.

## Schema (`silver_sleep`)

One row per night.

| Column | Type | Source / note |
|---|---|---|
| `night_date` | DATE | the calendar night (join key) |
| `garmin_sleep_score` | INT | `dailySleepDTO.sleepScores.overall.value` (nullable on old nights) |
| `garmin_total_min` | DOUBLE | `sleepTimeSeconds / 60` |
| `garmin_deep_min` / `garmin_light_min` / `garmin_rem_min` / `garmin_awake_min` | DOUBLE | stage seconds / 60 |
| `garmin_avg_stress` | DOUBLE | `avgSleepStress` |
| `garmin_resp_avg` | DOUBLE | `averageRespirationValue` |
| `garmin_spo2_avg` | DOUBLE | `averageSpO2Value` (nullable) |
| `garmin_rhr` | INT | top-level `restingHeartRate` (sibling of `dailySleepDTO`) |
| `garmin_start_gmt` / `garmin_end_gmt` | TIMESTAMP | `sleepStart/EndTimestampGMT` |
| `whoop_performance_pct` / `whoop_efficiency_pct` / `whoop_consistency_pct` | DOUBLE | `score.sleep_*_percentage` |
| `whoop_resp_rate` | DOUBLE | `score.respiratory_rate` |
| `whoop_deep_min` / `whoop_rem_min` / `whoop_light_min` / `whoop_awake_min` | DOUBLE | `score.stage_summary.total_*_milli / 60000` |
| `whoop_disturbances` | INT | `score.stage_summary.disturbance_count` |
| `whoop_cycle_id` | BIGINT | linkage to recovery/strain (for later gold joins) |
| `whoop_start` / `whoop_end` | TIMESTAMP | `start` / `end` |
| `has_garmin` / `has_whoop` | BOOLEAN | per-night coverage |

`silver_sleep_garmin` is the `garmin_*` columns keyed one-per-night; `silver_sleep_whoop` is the
`whoop_*` columns plus `whoop_sleep_id` and `is_nap`, keyed one-per-sleep-id.

## Asset checks

Severities follow the bronze convention — structural/parse/dedup correctness = **ERROR**,
coverage/expectation drift = **WARN** — and all run off the `*_api` pools.

| Check | Asset | Severity | What |
|---|---|---|---|
| `garmin_night_unique_nonnull` | `silver_sleep_garmin` | ERROR | one row per `night_date`, never null |
| `whoop_id_unique_night_nonnull` | `silver_sleep_whoop` | ERROR | one row per `whoop_sleep_id`; `night_date` non-null |
| `sleep_night_unique_nonnull` | `silver_sleep` | ERROR | one row per `night_date`, never null (the whole point) |
| `sleep_value_ranges` | `silver_sleep` | ERROR | percentages 0–100; stage minutes ≥ 0 and < 24h (catches a unit bug) |
| `sleep_join_sanity` | `silver_sleep` | WARN | no fully-null row; recent (≥ 2025-12-18) single-source nights surfaced as a soft flag |
| `sleep_coverage_split` | `silver_sleep` | WARN | reports both / garmin-only / whoop-only counts |
| `garmin_coverage_vs_bronze` | `silver_sleep_garmin` | WARN | silver nights ≈ bronze distinct `calendarDate` (no silent drop) |

# Glucose (Lingo CGM)

`silver_glucose` is the first **single-source** table — a straight reduction of the sleep
template (no join). One row per CGM reading.

### Source & event date
Lingo bronze (`lingo/glucose`) is a 2-column CSV (reading time, `Measurement(mg/dL)`) plus the
bronze `dt` column, re-uploaded cumulatively so each reading recurs in many files (~13× raw
duplication). The reading time is **already local with its offset embedded**
(`2026-06-11T18:05-04:00`) — so the event date is just its local date; no UTC/wake-date subtlety
like Whoop.

### Deduplication — on the UTC instant
The same physical reading is re-exported under **different offset spellings** (up to 4×, e.g. after
a timezone change): ~163k distinct local strings map to only **~55k distinct UTC instants**, and
every duplicate carries an **identical** value (zero conflicts). So dedup keys on the **UTC
instant** — lossless; keying on the local string would ~3× inflate. The instant is computed
arithmetically as `reading_ts_local − tz_offset` (the offset string lacks seconds and won't cast to
`TIMESTAMPTZ`).

### Local-date caveat
For ~11% of instants the derived local **date** differs across an instant's offset spellings. The
**instant is canonical**; for the local fields (`reading_ts_local`, `reading_date`,
`tz_offset_minutes`) the **latest-captured** export's representation wins (consistent with silver
being a projection of current bronze). `mgdl` is never ambiguous. A null measurement is kept with
`mgdl` null (never dropped), matching the null-safe convention.

### Schema (`silver_glucose`)
One row per reading.

| Column | Type | Note |
|---|---|---|
| `reading_ts_utc` | TIMESTAMP | UTC instant — the dedup key / canonical identity |
| `reading_ts_local` | TIMESTAMP | local wall-clock of the reading |
| `reading_date` | DATE | local date (the day) |
| `tz_offset_minutes` | INT | signed UTC offset of the reading |
| `mgdl` | INT | `Measurement(mg/dL)` (nullable) |

### Asset checks
| Check | Severity | What |
|---|---|---|
| `glucose_reading_unique_nonnull` | ERROR | one row per `reading_ts_utc`; `reading_ts_utc`/`reading_date` non-null |
| `glucose_value_range` | ERROR | non-null `mgdl` within 10–600 |
| `glucose_coverage_vs_bronze` | WARN | silver readings ≈ bronze distinct instants; reports distinct days |

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

# Operations

## Scheduling

- `silver_sleep_daily` (06:00 UTC) rebuilds the three sleep assets after the day's bronze sleep
  lands (Garmin daily + Whoop hourly).
- `silver_glucose_daily` (06:30 UTC) rebuilds `silver_glucose` (Lingo arrives via sensor; a daily
  rebuild keeps silver a current projection without chasing each upload).
- `silver_workouts_daily` (06:45 UTC) rebuilds `silver_workouts` (after the Garmin daily capture).
- `silver_checks_daily` (07:00 UTC) runs **all** silver checks (sleep + glucose + workouts)
  independently, so a *stopped* silver asset is still caught.

All are off by default; enable them in the UI. Rebuild on demand (e.g. after a bronze backfill)
with `dagster job execute --job silver_sleep_job` (or `--job silver_glucose_job`).

## Deployment

See [DEPLOYMENT.md → Silver](DEPLOYMENT.md#silver-cross-subject-layer) and
[ENV_TEMPLATE.md](ENV_TEMPLATE.md): bronze mounted **read-only**, `SILVER_ROOT` writable on a
separate volume, and a reserved `SILVER_MONITOR_DIR` (for the forthcoming silver monitor; unused
today).
