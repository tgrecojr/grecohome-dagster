# ADR 0007: Silver sleep — two co-equal sources, FULL OUTER JOIN, neither authoritative

## Status
Accepted.

## Context
With bronze complete, the first silver asset is **unified daily sleep** — the pattern-setter for
the whole silver layer (event-date extraction, dedup, typing, Parquet, asset checks). Two bronze
sleep sources exist, profiled against live data: **Garmin** (flat `dailySleepDTO`, ~1,329 nights
over ~4 years) and **Whoop** (`records[]`-wrapped, ~175 real nights + 236 naps since the device
was acquired ~2025-12-18). They overlap on recent nights (the user wears both) and measure
overlapping but non-identical things. The obvious-but-wrong move is to blend them into one
"true" sleep number; we need to decide how silver holds two independent measurements of the same
night.

## Decision
- **Keep both sources side by side, neither authoritative.** `silver_sleep` is a **FULL OUTER
  JOIN** of two deduped source assets on `night_date`, with `garmin_*` and `whoop_*` columns both
  present and nullable. Nothing is coalesced; no "primary/best" value is synthesized. Per-night
  `has_garmin` / `has_whoop` provenance makes every null explainable. No wearable observes sleep
  directly — each is an *estimate*, and the disagreement between them is signal that blending
  would destroy. Gold chooses a device per question, or compares the two; silver only holds both.
- **Three assets, the copyable shape.** `silver_sleep_garmin` and `silver_sleep_whoop` type +
  dedup each source; `silver_sleep` joins them. Lineage is explicit via **`AssetKey` deps** on the
  bronze sleep assets (which live in other code locations); reads are **filesystem reads** of
  `BRONZE_ROOT`, so the silver image depends only on `grecohome-core` + DuckDB.
- **Cross-subject `silver` code location.** Sleep spans two subjects, so it ships as its own image
  (not inside `whoop`/`garmin`). Generic transform helpers live in `grecohome_core.silver`; the
  sleep column mapping in `grecohome_silver.sleep`.
- **Event date from the payload, not `dt`.** Garmin night = `calendarDate`; Whoop night =
  `CAST(start AS DATE)`.
- **Dedup is mandatory** (bronze is heavily re-captured): Garmin by `calendarDate` keeping the
  latest fetch (tie-break on the filename `fetched_ms`); Whoop by `id` keeping the latest
  `updated_at` (rescores), then one non-nap record per night for the unified row.
- **Naps kept-but-flagged** (`is_nap`) in `silver_sleep_whoop`; excluded from the per-night
  unified row.
- **Units normalized to minutes** across both sources (Garmin seconds, Whoop millis).
- **Parquet under `SILVER_ROOT`, fully overwritten each run.** Silver is a rebuildable projection
  of current bronze; the writer refuses any path under `BRONZE_ROOT`. Whole-table (unpartitioned)
  for v1 — the data is small.
- **Asset checks** extend the bronze pattern (uniqueness/range = ERROR, coverage = WARN), off the
  `*_api` pools.

## Consequences
- Silver gains a new `SILVER_ROOT` (and a reserved, unused `SILVER_MONITOR_DIR` mirroring
  `bronze_monitor_dir`, for the forthcoming silver monitor) and a fifth published image
  (`grecohome-dagster-silver`) wired into the CI matrix and `workspace.yaml`.
- DuckDB enters the dependency set, but only in the silver image — the bronze capture images stay
  lean (no query engine).
- Garmin-only nights (pre-Whoop, ~4 yrs) carry null `whoop_*`; recent nights carry both. There is
  **no hidden device-switch artifact** in any column, because nothing is coalesced.
- The hard shape (two co-equal sources) is solved first; later silver tables are mostly
  single-source reductions of this template.

## Related
[[0001-bronze-only]], [[0002-dagster-pins]], [[0006-soil-port]]. Layer guide: `docs/SILVER.md`.
