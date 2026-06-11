# Silver layer

The silver layer is the typed, deduplicated, analysis-ready projection of bronze. It reads the
raw immutable bronze payloads, extracts the **true event date**, unnests and types fields,
deduplicates to **one row per logical record**, and writes columnar **Parquet**. Silver is
*derived and rebuildable* — it can always be dropped and regenerated from bronze, so it is not
immutable and not precious. **Bronze remains the only source of truth**; silver never touches it.

Scope today: the first silver asset, **unified daily sleep** (`silver_sleep`). It is the
pattern-setter — event-date extraction, dedup, typing, Parquet, a two-source join, and asset
checks — that later single-source silver tables (glucose, workouts, fitness) reduce from.

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
{SILVER_ROOT}/sleep/silver_sleep.parquet   # the product (one row per night)
{SILVER_ROOT}/sleep/_garmin.parquet        # source intermediate (one row per night)
{SILVER_ROOT}/sleep/_whoop.parquet         # source intermediate (one row per sleep id)
```

- **Atomic overwrite.** Each asset `COPY`s to a temp file in the destination dir and `os.replace`s
  it into place, so a crashed run never leaves a half-written Parquet.
- **Partitioning: none (v1).** The data is small (thousands of nights); each asset is a
  whole-table rebuild reading all bronze partitions. Partition by year later only if it grows.

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
- **Whoop:** the **local** date of `start` — `start` is UTC and carries a
  `timezone_offset`, so the night is `CAST(start + timezone_offset AS DATE)`. A bedtime
  in the evening local time is after midnight UTC for a negative offset, so a naive
  `CAST(start AS DATE)` in UTC dates ~93% of nights a day late (measured against live
  bronze) and misaligns with Garmin's local `calendarDate`. The offset's minutes inherit
  the hours' sign (`-04:30` → −4h −30m); a missing/unparseable offset falls back to the
  UTC date so the night is never dropped.

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

## Scheduling

- `silver_sleep_daily` (06:00 UTC) rebuilds all three assets after the day's bronze sleep lands
  (Garmin daily + Whoop hourly).
- `silver_checks_daily` (07:00 UTC) runs the silver checks independently, so a *stopped* silver
  asset is still caught.

Both are off by default; enable them in the UI. Rebuild on demand (e.g. after a bronze backfill)
with `dagster job execute --job silver_sleep_job`.

## Deployment

See [DEPLOYMENT.md → Silver](DEPLOYMENT.md#silver-cross-subject-layer) and
[ENV_TEMPLATE.md](ENV_TEMPLATE.md): bronze mounted **read-only**, `SILVER_ROOT` writable on a
separate volume, and a reserved `SILVER_MONITOR_DIR` (for the forthcoming silver monitor; unused
today).
