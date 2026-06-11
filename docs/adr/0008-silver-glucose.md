# ADR 0008: Silver glucose — per-reading grain, dedup on the UTC instant

## Status
Accepted.

## Context
Glucose (Lingo CGM) is the second silver table and the first **single-source** one — a reduction
of the sleep template (no join). The Lingo bronze is a cumulative CSV re-uploaded constantly, so
each reading recurs in many files. Two profiling facts (against live bronze) shaped the design:

1. The reading time is **already local with its UTC offset embedded** (`2026-06-11T18:05-04:00`),
   so the event date is just its local date — none of the UTC/wake-date subtlety sleep had.
2. The same physical reading is re-exported under **different offset spellings** (up to 4×, e.g.
   after a timezone change): ~163k distinct local strings collapse to ~**55k distinct UTC
   instants**, with **zero conflicting values**.

## Decision
- **Grain: one row per reading** (`silver_glucose`). Silver is a faithful typed projection;
  daily time-in-range / mean / variability **aggregates are analysis and belong in gold**, not
  silver. This keeps the layer boundary clean and avoids pre-committing to one analysis grain.
- **Dedup on the UTC instant**, not the local string. The instant is the reading's true identity;
  keying on the local string would ~3× inflate. The instant is derived **arithmetically**
  (`reading_ts_local − tz_offset`), because the offset string has no seconds and won't cast to
  `TIMESTAMPTZ`.
- **Columns:** `reading_ts_utc` (the key), `reading_ts_local`, `reading_date`,
  `tz_offset_minutes`, `mgdl`. Null measurements are kept with `mgdl` null (never dropped).
- **Local-date tie-break:** for ~11% of instants the derived local *date* differs across offset
  spellings. The instant is canonical; the local fields take the **latest-captured** export's
  representation (silver = projection of current bronze). `mgdl` is never ambiguous, so the value
  is unaffected. Revisit if a more principled per-reading timezone source appears; cheap to change
  since silver rebuilds.
- **Same code location/image.** Glucose rides in the existing cross-subject `silver` location;
  no new image. Reads `lingo/glucose` from `BRONZE_ROOT` via DuckDB; lineage on
  `lingo_bronze_glucose` declared by `AssetKey`. A new CSV reader
  (`grecohome_core.silver.csv_relation_sql`) is the only shared addition.
- **Checks:** uniqueness on the instant + non-null key (ERROR), `mgdl` range 10–600 (ERROR),
  coverage vs bronze distinct instants (WARN). Daily rebuild job/schedule; off the `*_api` pools.

## Consequences
- ~55k readings today (2025-05-10 →), one Parquet at `{SILVER_ROOT}/glucose/silver_glucose.parquet`.
- Intermittent sensor wear means gaps are normal (206 days of ~398) — a coverage signal, not an
  error.
- The CSV reader generalizes the silver framework beyond JSON, ready for future CSV sources.
- Daily glucose aggregates remain a gold concern, joinable to `silver_sleep` by local date.

## Related
[[0001-bronze-only]], [[0005-lingo-port]], [[0007-silver-sleep]]. Layer guide: `docs/SILVER.md`.
