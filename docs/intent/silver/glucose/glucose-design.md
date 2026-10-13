---
parent: silver
prefix: SILVER-GLUCOSE
---

# Silver glucose

## Context

`silver_glucose` is the typed, deduplicated projection of the Abbott Lingo continuous glucose
readings: **one row per reading**. It reads `lingo/glucose` bronze, where each capture is a
whole Lingo CSV export (two columns: the reading time and `Measurement(mg/dL)`).

Exports are **cumulative**: every export repeats every reading to date, and each export is its
own bronze capture. So one physical reading appears in many bronze files, and deduplication
carries the table.

The grain stays per reading. Daily time-in-range, mean or variability are analysis, which
belongs in gold; silver doesn't pre-commit to one aggregation.

## Assets

| Asset | Grain | Reads | Writes |
|---|---|---|---|
| `silver_glucose` | One row per reading (per UTC instant) | `lingo/glucose` (bronze dependency `lingo_bronze_glucose`) | `{SILVER_ROOT}/glucose/silver_glucose.parquet` |

Each run records `rows`, `bronze_files` and `path` as metadata.

## The reading time

The CSV's `Time of Glucose Reading [T=(local time) +/- (time zone offset)]` column holds the
local wall-clock time with its UTC offset embedded, for example `2026-06-11T18:05-04:00`. From
it:

- `reading_ts_local` is the local wall-clock (`YYYY-MM-DDTHH:MM`), as a naive `TIMESTAMP`;
- `reading_date` is its local date, the day the reading belongs to;
- `tz_offset_minutes` is the signed offset, the minutes taking the hours' sign (`-04:30` is
  −270);
- `reading_ts_utc` is the **UTC instant**, computed as local time minus the offset. It's
  computed arithmetically because the offset has no seconds field and won't cast to
  `TIMESTAMPTZ`.

A row whose local time doesn't parse is dropped. A row whose local time parses but whose offset
doesn't (no offset, a `Z`, or a seconds field shifting the offset's position) is kept with
`reading_ts_utc` and `tz_offset_minutes` null. Deduplication then collapses every such row into
one null-key row, which fails `glucose_reading_unique_nonnull` at ERROR: a change in the export's
time format pages rather than passing silently. Lingo's exports have always used the
`YYYY-MM-DDTHH:MM±HH:MM` form.

The local date needs no wake-date or offset logic, unlike Whoop sleep: the source already states
local time. During a daylight-saving fall-back, the repeated local hour maps to distinct instants,
so both readings are kept and the local time isn't unique; the instant is.

**Columns are read by header name**, so an extra or reordered column in an export is harmless.
If the reading-time or measurement header were renamed in some exports, those exports' rows would
be dropped without a coverage signal (the coverage count reads the same columns); if renamed in
every export, the rebuild fails. Lingo's bronze schema-drift check (`lingo_glucose_schema_drift`,
whose signature is the header row) is what reports a header change, at ERROR.

## Deduplication

**One row per UTC instant**, keeping the **latest capture** (the 13-digit fetch time in the
bronze filename).

The instant, not the local string, is a reading's identity. The same physical reading is
re-exported under different offset spellings (after a time-zone change, for example), so
distinct local strings far outnumber distinct instants, and every duplicate carries the same
value. Keying on the instant is lossless; keying on the local string would multiply readings
roughly threefold.

**Local fields of a re-spelled reading.** For some instants, the spellings disagree on the
local date. The instant stays canonical, and every column, `mgdl` included, comes from the latest
capture, consistent with silver projecting current bronze. Captures that tie on fetch time are
picked arbitrarily. Profiling in 2026-06 found no instant whose spellings disagreed on the value,
so the choice only affects the local fields.

## Values

`mgdl` is the measurement cast to `INT`, so a fractional value is rounded. A missing or
non-numeric measurement keeps the reading with `mgdl` `NULL`; the reading is never dropped.

## Schema

| Column | Type | Meaning |
|---|---|---|
| `reading_ts_utc` | TIMESTAMP | The UTC instant: the key |
| `reading_ts_local` | TIMESTAMP | Local wall-clock time |
| `reading_date` | DATE | Local date |
| `tz_offset_minutes` | INT | Signed UTC offset, in minutes |
| `mgdl` | INT | Glucose in mg/dL (nullable) |

## Asset checks

| Check | Severity | Passes when |
|---|---|---|
| `glucose_reading_unique_nonnull` | ERROR | `reading_ts_utc` is unique, and neither it nor `reading_date` is null |
| `glucose_value_range` | ERROR | Every non-null `mgdl` is within 10–600 |
| `glucose_coverage_vs_bronze` | WARN | Silver's row count ≥ bronze's distinct parseable instants; also reports distinct days. A drop means dedup or a filter is losing readings |
| `silver_glucose_freshness` | ERROR | The Parquet was written within the last 36 hours (the layer's freshness rule) |

Every check fails at ERROR, and pages, if the table's Parquet doesn't exist. Gaps between days
are normal (the sensor is worn in bursts); they're a coverage signal, not a failure, and bronze's
completeness check reports them.

## Schedule

`silver_glucose_job` rebuilds `silver_glucose` and runs its checks daily at **06:30 UTC**
(`silver_glucose_daily`, which starts stopped). Exports arrive whenever the user uploads one;
a daily rebuild keeps silver current without chasing each upload.

## Decisions & Alternatives

| Decision | Chosen | Alternatives Considered | Rationale |
|---|---|---|---|
| Grain | One row per reading | Daily aggregates | Aggregates are analysis (gold); a per-reading table serves every later aggregation. |
| Dedup key | The UTC instant | The local timestamp string | The same reading recurs under up to four offset spellings with identical values: profiled in 2026-06, about 163k distinct local strings were about 55k instants, with no conflicting values. The instant is the reading's identity. |
| UTC instant | Local time minus the parsed offset | Cast the string to `TIMESTAMPTZ` | The offset lacks seconds and won't cast. |
| Local fields on disagreement | Latest capture's spelling | Earliest; the most common spelling; a per-reading time-zone source | Silver projects current bronze; the value is unaffected. About 11% of instants disagreed on the local date when profiled. A principled per-reading time-zone source would be better if one appears, and changing this is cheap because silver rebuilds. |
| Null measurement | Keep the reading, `mgdl` null | Drop it | Null-safe convention: a reading's existence is data. |

## References

- `packages/silver/src/grecohome_silver/glucose.py`, `dagster/glucose_assets.py`,
  `dagster/glucose_checks.py`
- [Lingo design](../../lingo/lingo-design.md): the cumulative exports and per-file capture
