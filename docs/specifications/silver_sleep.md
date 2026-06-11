# Specification: Silver Layer — Sleep (first silver asset)

**Target repo:** `grecohome-dagster` (uv monorepo; `grecohome-core` + subject packages).
**Goal:** Build the **first silver asset — unified daily sleep** — as Dagster asset(s) that depend
on the bronze sleep assets, establishing the silver pattern (event-date extraction, dedup, typing,
Parquet output, asset checks) that all later silver assets will copy.
**Dagster:** pinned `dagster==1.13.8` / `dagster-*==0.29.8`. Use only APIs available there.
**This is the pattern-setter.** Optimize for a clean, copyable template over breadth.

---

## 0. What silver is (and what it is not)

Silver is the transformation bronze deliberately skips: read raw immutable payloads, extract the
**true event date**, unnest/type fields, **deduplicate to one row per logical record** (keeping the
latest restatement), and write **typed columnar Parquet** that analysis can be run against. Silver
is *derived and rebuildable* — it can always be dropped and regenerated from bronze, so it is not
immutable and not precious. Bronze remains the only source of truth.

Non-goals: no gold/marts; no analysis or correlations (that's gold); no touching bronze or capture;
not "clean every bronze stream" — only sleep here.

---

## 1. Source decision (grounded in live bronze profiling)

Two bronze sleep sources exist, with different depth (verified against live data):

- **Garmin sleep** — flat `dailySleepDTO` payload. **1,329 distinct nights, 2022-06-06 → present
  (~4 years).** Fields: overall sleep score, deep/light/REM/awake seconds, avg sleep stress,
  respiration (avg/low/high), SpO2 (avg/low/high), plus sibling top-level `restingHeartRate`.
- **Whoop sleep** — `records[]`-wrapped. **175 distinct real nights + 236 naps, 2025-12-18 →
  present (~6 months).** Fields: sleep performance/consistency/efficiency %, respiratory rate,
  stage-summary millis, disturbance count, sleep-need breakdown, and (via `cycle_id`) linkage to
  Whoop recovery/strain.

**Design: two co-equal sources, preserved side by side — NEITHER is authoritative.** Rationale:
no wearable measures sleep with full accuracy; each is an independent *estimate* of a night you
cannot directly observe. Blending them into one "true" value launders two different methodologies
into a falsely-authoritative number and throws away the disagreement between them — and that
disagreement is itself signal (nights where the devices diverge are meaningful). The user wears
both devices on most nights and explicitly wants both measurements retained, not collapsed.

So silver sleep keeps **both sources' columns side by side, both nullable**, joined by night via a
**FULL OUTER JOIN** (§4.6): every night either device recorded is one silver row. Garmin carries
the full ~4-year history; Whoop columns are simply null before the user owned the device
(~2025-12-18). No source is privileged, no values are coalesced, no "primary/best" column is
synthesized. Gold-layer analysis later chooses a device per question, or compares the two (e.g.
device deltas, agreement) — silver's job is only to faithfully hold both. Build in three assets
(§2).

**Note — no cross-device discontinuity.** Because nothing is coalesced, there is no hidden
device-switch artifact in any column: a `garmin_*` column is always Garmin's methodology end to
end, a `whoop_*` column always Whoop's. The only "gap" is the obvious one — `whoop_*` is null
before the device existed — which `has_whoop` makes explicit.

---

## 2. Asset structure (3 assets, the copyable pattern)

```
silver_sleep_garmin   (bronze garmin/sleep  -> typed, deduped, one row/night)
silver_sleep_whoop    (bronze whoop/sleep   -> typed, deduped, one row/night, naps flagged)
silver_sleep          (FULL OUTER JOIN garmin + whoop on night -> unified, both sides side-by-side)
```

- Each is a Dagster `@asset` declaring its bronze upstream via `deps=` (or `AssetIn`), so lineage
  is explicit: `bronze garmin/sleep -> silver_sleep_garmin -> silver_sleep`, same for whoop.
- Place under `grecohome-core` shared silver helpers + a `sleep` silver module. Follow the existing
  core-vs-subject split: generic transform helpers (dedup, event-date, parquet-write) in core; the
  sleep-specific column mapping in the sleep silver module. (Sleep spans two subjects, so the
  unified asset naturally lives in core/a cross-subject silver package rather than inside whoop or
  garmin alone — confirm against how the repo is organized and choose the consistent home.)

---

## 3. Storage & format conventions (decide once; all silver follows)

- **Format: Parquet.** Typed, columnar, compressed. Written via DuckDB `COPY (...) TO '...'
  (FORMAT parquet)` or equivalent.
- **Location:** a silver root **outside `BRONZE_ROOT`**, passed by config (`SILVER_ROOT`, mirroring
  the swappable-root bronze convention; keep object-store migration open). Suggested layout:
  `{SILVER_ROOT}/sleep/silver_sleep.parquet` (+ the two source-level intermediates, e.g.
  `{SILVER_ROOT}/sleep/_garmin.parquet`, `_whoop.parquet`). Document in `docs/SILVER.md`.
- **Partitioning:** silver may be a single Parquet per asset to start (the data is small — thousands
  of nights). If/when it grows, partition by year. Do NOT over-engineer partitioning now.
- **Rebuildable:** silver assets fully overwrite their output on materialization (idempotent
  rebuild from bronze). No append/merge semantics; last run wins. This keeps silver a pure
  projection of current bronze.
- **Never write under `BRONZE_ROOT`.**

---

## 4. Transform rules (the pattern every silver asset copies)

### 4.1 Sidecar exclusion (carry over from bronze checks)
Every bronze read excludes `*.meta.json`. For row reads, `WHERE filename NOT LIKE '%.meta.json'`.
Reuse the `bronze_reads` helpers built for the validation checks — do not re-solve this.

### 4.2 Event date extraction (the core silver job)
- **Garmin:** event date = `dailySleepDTO.calendarDate` (already a clean DATE string). This is the
  authoritative night. (Do NOT use bronze `dt` — though for Garmin they coincide, always derive
  from payload for correctness and consistency.)
- **Whoop:** event date = the **sleep night**. Whoop `start`/`end` are timestamps; a sleep that
  starts late evening belongs to that night. Define night = `CAST(start AS DATE)` (or, if you find
  cross-midnight cases need it, the date of `end` — pick one rule, document it, apply consistently).
  Whoop `dt` is partition date and must NOT be used as the event date.

### 4.3 Deduplication (critical — bronze is heavily re-captured)
Live data shows massive duplication (every night captured many times across pulls):
- **Garmin:** dedup key = `calendarDate`. Keep the **latest** version. Garmin is immutable per
  BRONZE.md, but the same night appears in many files (re-pulls), so dedup by calendarDate keeping
  one is still required. Tie-break by sidecar `fetched_at` (latest) or by the file's `dt`.
- **Whoop:** dedup key = `r.id` (the sleep UUID). Whoop **rescores**, so keep the row with the
  **latest `updated_at`** per `id`. Then collapse to one row per night (see naps, §4.4).
Use a `row_number() OVER (PARTITION BY <key> ORDER BY <recency> DESC) = 1` pattern.

### 4.4 Naps (Whoop-specific decision)
Whoop has 236 nap records (`nap = true`) vs 175 real nights. **Keep naps in `silver_sleep_whoop`
but flag them** (`is_nap` boolean). For the unified `silver_sleep` (one row per night), use only
`nap = false` records as the night. Do not silently drop naps from the source-level asset — they're
real data; just exclude them from the per-night unified row. (Garmin sleep here is the main nightly
record; if Garmin nap data exists separately, it's out of scope for v1.)

### 4.5 Typing & unit normalization
- Convert Garmin `*Seconds` sleep stages to a consistent unit. **Recommendation: store minutes**
  (or seconds) — pick one and apply to BOTH sources. Whoop stages are in **millis**
  (`total_*_time_milli`); Garmin in **seconds**. Normalize both to the same unit (minutes) so the
  unified columns are comparable. Document the chosen unit.
- Dates as DATE, timestamps as TIMESTAMP (store Whoop `start`/`end` as UTC timestamps too, for
  later "what time did I sleep" questions).
- Scores/percentages as typed numerics. Null-safe: older Garmin nights have null overall score
  (762 files) — keep the night, null the score, never drop.

### 4.6 Unified join (`silver_sleep`)
- **FULL OUTER JOIN** of the two deduped source assets on `night_date`. Every night either device
  recorded is exactly one row. Neither source is the spine; both sets of columns are present and
  nullable. Garmin-only nights (pre-Whoop, ~4 yrs) have null `whoop_*`; any Whoop-only night has
  null `garmin_*`; nights with both (most recent nights, since the user wears both) have both.
- **Keep both sources' columns side by side, namespaced** (`garmin_*` and `whoop_*`). Do NOT
  coalesce, do NOT synthesize a "primary/best" value, do NOT prefer one device. The two are
  independent measurements and are retained as such.
- Include per-night provenance: `has_garmin` (BOOLEAN), `has_whoop` (BOOLEAN). These are the
  coverage indicators that make every null explainable and that let gold compute device deltas /
  agreement later.
- Grain stays **one row per `night_date`** because each source asset is already deduped to one row
  per night, so the full outer join is one-to-one per night.

---

## 5. Suggested silver_sleep schema (confirm/adjust against payloads)

One row per night. Columns (names illustrative — match repo conventions):
```
night_date            DATE        -- the calendar night (join key)
-- Garmin source (independent measurement; full ~4yr history)
garmin_sleep_score    INT         -- dailySleepDTO.sleepScores.overall.value (nullable)
garmin_total_min      DOUBLE      -- sleepTimeSeconds/60
garmin_deep_min       DOUBLE
garmin_light_min      DOUBLE
garmin_rem_min        DOUBLE
garmin_awake_min      DOUBLE
garmin_avg_stress     DOUBLE      -- avgSleepStress
garmin_resp_avg       DOUBLE      -- averageRespirationValue
garmin_spo2_avg       DOUBLE      -- averageSpO2Value (nullable)
garmin_rhr            INT         -- top-level restingHeartRate (sibling of dailySleepDTO)
garmin_start_gmt      TIMESTAMP   -- sleepStartTimestampGMT
garmin_end_gmt        TIMESTAMP
-- Whoop source (independent measurement; null before device owned, ~2025-12-18)
whoop_performance_pct DOUBLE      -- score.sleep_performance_percentage
whoop_efficiency_pct  DOUBLE
whoop_consistency_pct DOUBLE
whoop_resp_rate       DOUBLE      -- score.respiratory_rate
whoop_deep_min        DOUBLE      -- total_slow_wave_sleep_time_milli/60000
whoop_rem_min         DOUBLE
whoop_light_min       DOUBLE
whoop_awake_min       DOUBLE
whoop_disturbances    INT         -- score.stage_summary.disturbance_count
whoop_cycle_id        BIGINT      -- linkage to recovery/strain (for later gold joins)
-- provenance
has_garmin            BOOLEAN
has_whoop             BOOLEAN
```

---

## 6. Asset checks on silver (extend the bronze-check pattern)

Each silver asset gets `@asset_check`s (the pattern you just built for bronze):
- **Uniqueness (ERROR):** one row per `night_date` in `silver_sleep` (dedup correctness — the whole
  point). Likewise one row per key in each source-level asset.
- **Range validity (ERROR):** scores/percentages in plausible bounds (sleep score 0–100, stage
  minutes ≥ 0 and < 24h, percentages 0–100). Catches a parsing/unit bug.
- **Row count / coverage (WARN):** silver night count is within expectations vs bronze distinct
  nights (e.g. silver_sleep_garmin distinct nights ≈ bronze garmin/sleep distinct calendarDate).
  A big drop means dedup or a filter is wrong.
- **Join sanity (WARN):** every silver_sleep row has at least one source (`has_garmin OR
  has_whoop` is always true — no fully-null rows). Additionally, since the user wears both devices
  on most recent nights, a recent night (≥ 2025-12-18) with only ONE source is worth a soft flag —
  it may indicate a sync/wear gap on the other device.
- **Coverage split (WARN):** report counts of garmin-only / whoop-only / both nights, so the
  source mix is visible each run (a sudden shift — e.g. recent nights going garmin-only — signals a
  Whoop capture problem).
- **Non-null key (ERROR):** `night_date` never null in any silver asset.
Keep severities consistent with the bronze convention: structural/parse errors = ERROR,
coverage/expectation = WARN. Run off the API concurrency pools (silver makes no source calls).

---

## 7. Dagster wiring (1.13.8)

- Define `silver_sleep_garmin`, `silver_sleep_whoop`, `silver_sleep` as `@asset`s with explicit
  `deps` on the bronze sleep assets so lineage renders.
- Partitioning: the bronze sleep assets are daily-partitioned. silver_sleep is a **whole-table**
  rebuild (not partitioned) for v1 — it reads all bronze partitions and rewrites one Parquet. If
  you prefer partitioned silver later, that's a follow-up; do not block v1 on it. (Confirm whether
  reading across all bronze daily partitions from an unpartitioned silver asset fits the repo's
  asset patterns; if partitioned silver is cleaner given how bronze is partitioned, use a single
  catch-all rebuild job/schedule rather than per-partition silver for the unified table.)
- Add a job + schedule to materialize silver sleep on a cadence (e.g. daily, after the day's bronze
  sleep lands). It depends on bronze having materialized; a simple daily schedule a few hours after
  the Garmin daily/ Whoop hourly captures is sufficient. Keep it OFF `garmin_api`/`whoop_api` pools.
- Register assets + checks in the appropriate `Definitions`.

---

## 8. Testing & acceptance

- **Unit tests** (core, `uv run pytest`) on a synthetic bronze tree with: duplicate captures of the
  same night (assert dedup → one row), a Whoop rescore (two `updated_at` for one `id` → keep
  latest), a nap record (assert flagged in source asset, excluded from unified night), an old
  Garmin night with null score (assert kept, score null), a Whoop-only-overlap night (assert join),
  and `.meta.json` sidecars present (assert excluded).
- **Live validation** (manual, via DuckDB): after first materialization, assert
  `silver_sleep` distinct `night_date` ≈ bronze Garmin distinct `calendarDate` (~1,329, growing),
  and Whoop overlap rows ≈ 175 within the post-Dec-2025 window.
- `uv run ruff check` clean; `uv run pytest` green.

Acceptance:
- [ ] `silver_sleep_garmin`, `silver_sleep_whoop`, `silver_sleep` materialize; lineage shows bronze
      deps in the UI.
- [ ] One row per night in `silver_sleep` (uniqueness check passes); ~4 years of nights present.
- [ ] Whoop columns populated only on overlapping nights; null before 2025-12-18.
- [ ] Stage units normalized to one unit across both sources; range checks pass.
- [ ] Silver written as Parquet under `SILVER_ROOT`, never under `BRONZE_ROOT`; rebuild is
      idempotent (re-materialize → same result).
- [ ] Silver asset checks present and green (uniqueness, ranges, coverage), off the API pools.
- [ ] `docs/SILVER.md` documents the source decision, event-date rules, dedup keys, chosen unit,
      and storage layout.

---

## 9. Gotchas carried from bronze work (do not re-learn these)

- **Sidecar `.meta.json` exclusion** on every bronze read; reuse the validation-check read helpers.
- **`dt` ≠ event date.** Derive the night from payload (`calendarDate` / Whoop `start`), never the
  partition folder. (For Garmin they coincide; still derive from payload for consistency.)
- **Heavy duplication in bronze** — dedup is mandatory, not optional; verify distinct-night counts
  after (Garmin ~1,329, Whoop ~175 nights) so a broken dedup is caught immediately.
- **Unit mismatch:** Whoop stages are millis, Garmin seconds — normalize both to one unit.
- **Null-safe on old data:** older Garmin nights lack scores — keep the night, null the field.
- **Silver is rebuildable & lives outside `BRONZE_ROOT`**; bronze stays untouched.
- **Validation/silver runs stay off `garmin_api`/`whoop_api` concurrency pools.**

---

## 10. Why this asset first / why this shape

Sleep is central to 4 of the 5 driving questions and has the deepest data (Garmin ~4 yrs). Building
it first establishes the entire silver template — event-date extraction, dedup, typing, Parquet,
join-of-two-sources, and silver asset checks — that glucose, workouts, and fitness silver will
copy. The two-co-equal-source FULL OUTER JOIN (both devices side by side, neither authoritative)
is the non-trivial pattern (most other silver tables will be single-source and simpler), so getting
the hardest shape right first means the rest are reductions of this template, not new problems.