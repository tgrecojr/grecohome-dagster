---
parent: high-level-design
prefix: SOIL
---

# Soil (NOAA USCRN) bronze subject

## Context and Design Philosophy

`grecohome_soil` (code location `soil_ingest`) captures hourly soil and weather observations
from one NOAA U.S. Climate Reference Network (USCRN) station to bronze. In bronze and in its
asset names the source is called `uscrn`; the package and spec prefix are `soil`. It replaced
the standalone `soildata` service, which re-downloaded the year file hourly and upserted rows
into Postgres.

The source is unlike the other subjects:

- **A public file, no auth.** The `hourly02` product is one headerless, whitespace-delimited
  file per station-year, `CRNH0203-{year}-{station}.txt`, under
  `https://www.ncei.noaa.gov/pub/data/uscrn/products/hourly02/{year}/`. It gains one row per
  hour. There's no token or key, and nothing to mount but bronze.
- **The whole year is one file.** Its content changes every hour, so storing the whole file per
  fetch would re-store the year on every tick, and skip-identical would never fire.
- **NOAA rewrites the year files in place, indefinitely.** Hours first published as "no
  transmission" rows (`CRX_VN = -9.000`, every value a sentinel) are filled in later from the
  station's logger, and corrupted rows are repaired. Corrections have been seen two years after
  the fact. **A day is never final.**

The design follows from those three facts. Each daily partition stores **only that UTC day's
rows**, sliced from the year file. Recent days are re-captured often, and a long trailing
window is re-sliced weekly, so late corrections land in bronze. Skip-identical keeps
re-captures of unchanged days free (HLD: *Completeness over economy*; *Bronze re-captures a
trailing window*).

The station is configurable, but there is one (`USCRN_STATION`, default `PA_Avondale_2_N`),
consistent with the single-user constraint. It's recorded in the sidecar, not the path.

## Capture

### One partition, one UTC day

`uscrn_bronze_hourly` is one asset over daily UTC partitions from `USCRN_START_DATE` (default
`2010-01-01`), with `end_offset=1`, so the current, in-progress day is a partition re-captured
intraday as rows post. For a partition date `YYYYMMDD`, the asset:

1. **Fetches the year file(s)** that can hold that date's rows: the date's own year, and for
   **Jan 1** also the previous year, read first so the day assembles in hour order. A row is
   stamped with the hour it *ends* (`UTC_TIME`), and the `YYYY0101 0000` row (the hour 23:00 to
   24:00 on Dec 31) is published as the last row of the *previous* year's file. Every other Jan 1
   row is in the new year's file.
2. **Selects the rows** whose second whitespace-separated field (`UTC_DATE`) equals `YYYYMMDD`,
   in file order. Blank lines, and lines with fewer than two fields, are skipped. This is pure
   selection, with no parsing or reformatting of values. Each line's content is kept exactly;
   line endings are normalized to `\n`, and the text is stored as UTF-8. If a row ever appeared
   in both year files, it would be kept twice; silver's latest-capture-per-hour handling has to
   tolerate that.
3. **Captures** the rows, joined with `\n` plus a trailing newline, as `text/plain`, `ext=txt`,
   with **skip-identical on**, to `uscrn/hourly/dt=<partition>/`. A date with no rows yet (early
   in the UTC day) writes nothing.

The sidecar carries public source provenance only: `request_url` (the partition's own year
file, even on Jan 1), and `request_params` holding `station`, `wbanno` (the first field of the
first row), `year`, `year_files` (the files consulted: two on Jan 1, whether or not both
existed), `utc_date` and `row_count`.

A finished day therefore converges to one file. Today's folder can hold a few progressively
larger slices, each a faithful snapshot of what NOAA showed when it was captured. A day NOAA
later corrects gets a new capture, and silver takes the latest capture per hour.

### Fetching

- One `GET` per year file with httpx: a 30 s timeout, redirects not followed, and a
  `grecohome-soil/<version>` User-Agent.
- **A 404 is a graceful skip.** The year file doesn't exist yet (early in a new year, or a
  backfilled year before the station existed), so it's treated as "no rows" and logged as a
  `uscrn year file not found` WARNING (service log, not the Dagster run log). Wide backfills
  stay robust.
- **Anything else fails the run, with no retry inside it:** another HTTP error status, a 3xx, a
  timeout or a connection error. The next tick is the retry. A 200 whose body holds no matching
  rows (empty, or an HTML page) writes nothing.
- **Within a run, each year file is fetched once** and cached, including a 404. A run covering
  many partitions slices all of them from the same download, so re-slicing 400 days costs about
  two GETs, not 400. It's three when the window reaches back into the year before last, which
  happens for runs between Jan 1 and early February. The whole year file is still downloaded on
  every tick, because NOAA offers no incremental endpoint. Storage, not bandwidth, is what the
  day slice keeps flat.

### Range runs

The asset is **partition-range aware**: a run may cover one partition or a contiguous range,
and it captures each partition in order. UI and CLI backfills are split into runs of at most
**366 partitions**, which touch two or three year files each.

If a fetch fails partway through a range, the days already captured stay in bronze (each write
is atomic), the run fails without metadata, and Dagster marks every partition in the range
failed. The next weekly run re-slices the range, and skip-identical keeps the redo cheap. The run records `partitions`,
`first_partition`, `last_partition`, `rows`, `captured` (how many partitions wrote a file) and
`year_files` (each year fetched, `ok` or `not found (404)`) as metadata.

The asset tags the `uscrn_api` concurrency pool, whose limit is set on the host (if at all;
volume is low).

## Schedules

Both drive `uscrn_capture_job` (UTC).

| Schedule | Cron | Runs | `run_key` |
|---|---|---|---|
| `uscrn_schedule` (the live feed) | `0 */6 * * *` | One run per partition in the trailing `USCRN_LOOKBACK_DAYS` (default 2): today and yesterday, catching the UTC-midnight rollover and rows that post during the day | `<partition>-<YYYYMMDDTHH>` |
| `uscrn_correction_schedule` | `30 3 * * 0` (Sunday 03:30, clear of the 6-hourly ticks) | **One partition-range run** over the trailing `USCRN_CORRECTION_LOOKBACK_DAYS` (default 400), through Dagster's partition-range run tags | `correction-<first>-<last>-<YYYYMMDDTHH>` |

The tick-scoped keys make every tick a distinct run. Both capture schedules start **stopped**,
per the project rule (HLD: *Capture schedules and sensors start stopped*): enable
`uscrn_schedule` and `uscrn_correction_schedule` in the UI after the first deploy (listed in
`docs/DEPLOYMENT.md`). The checks schedule starts running.

400 days covers the current and the previous year file. NOAA corrections older than that need a
manual backfill. Re-slicing a range costs two downloads and writes only the days that changed.

## Bronze checks

One `CollectionCheckConfig` for `uscrn/hourly`, run by `uscrn_bronze_checks_job` hourly (a
checks-only schedule, `uscrn_bronze_checks_hourly`, which starts running):

| Setting | Value | Why |
|---|---|---|
| `reader` | `txt` | Raw text rows; schema drift uses the field count of the first row |
| `event_date_source` | `partition` | `dt` is the UTC date the rows cover |
| Freshness | 30 h cadence + 12 h grace | NOAA publishes with a lag and the live schedule runs every 6 h |
| Completeness | gap over 2 days (WARN) | |
| Schema drift, content health | on | |

There's no auth, so there's no auth-health check.

## Configuration

| Variable | Default | Meaning |
|---|---|---|
| `USCRN_STATION` | `PA_Avondale_2_N` | Station filename stem (`STATE_LOCATION_DISTANCE_DIRECTION`) |
| `USCRN_BASE_URL` | `https://www.ncei.noaa.gov/pub/data/uscrn/products/hourly02` | Product base URL |
| `USCRN_LOOKBACK_DAYS` | 2 | Live schedule's trailing partitions |
| `USCRN_CORRECTION_LOOKBACK_DAYS` | 400 | Correction run's trailing partitions |
| `USCRN_START_DATE` | `2010-01-01` | First daily partition |
| `BRONZE_ROOT`, `BRONZE_MONITOR_DIR` | (required; optional) | Shared bronze root and check-state directory |

There's nothing else to mount: no token, no key.

## What the other USCRN products can't do

These were evaluated as ways to fill gaps in the station's data. None of them can:

- **`updates/`** is the raw NOAAPort real-time broadcast: one file per load-hour with about 150
  stations' single hour. It's *upstream* of the year file and never corrected.
- **`snapshots/`** are weekly full-archive zips (about 840 MB).
- **`subhourly01`** (5-minute data) shows the same sentinels in gap hours.
- **`daily01`** applies its own completeness rule, dropping soil temperature below about 22
  valid hours.

Avondale is the only USCRN station within about 1.5°, so there's no spatial fill within USCRN.
Long gaps in the Avondale data (for example air temperature in May 2026, 50 cm soil moisture
since June 2026, afternoon soil readings since July 2026) are **sensor faults at the station**,
not ingest problems: the source itself carries sentinels for those hours.

## Decisions & Alternatives

| Decision | Chosen | Alternatives Considered | Rationale |
|---|---|---|---|
| Payload granularity | One UTC day's rows per partition | Store the whole year file per fetch | The whole file changes every hour, so skip-identical would never fire and every tick would re-store the year. A day slice converges to one file once the day stops changing. |
| Transformation in bronze | Pure row selection, byte-faithful | Parse into typed columns | Bronze stays raw; typing and QC-flag handling belong to silver. |
| Late corrections | A weekly 400-day partition-range re-slice | Only the 2-day live window; re-fetch everything daily | NOAA corrects days months to years later. A range run fetches each year file once, and skip-identical writes only changed days, so the sweep is cheap. 400 days spans the current and previous year file. |
| Jan 1 `0000` row | Also read the previous year's file for Jan 1 | Accept losing that hour | NOAA publishes that row as the last row of the previous year's file. Without it, every Jan 1 misses its first hour. |
| Missing year file | 404 is a WARNING and "no rows" | Fail the run | Early-year and pre-station dates legitimately have no file. Failing would break wide backfills. |
| Gap filling from other USCRN products | Not done | `updates/`, `snapshots/`, `subhourly01`, `daily01` | None of them holds data the year file lacks (see *What the other USCRN products can't do*). |
| Station | One, configurable, recorded in the sidecar | Multiple stations; station in the path | Single-user system. Keeping it out of the path leaves the layout stable if the station ever changes. |

## Open Questions & Future Decisions

### Deferred

1. **Corrections older than 400 days.** These are only picked up by a manual backfill. A full
   2010→2025 backfill (2026-09-22) found none, apart from each year's Jan 1 `0000` row.
2. **`daily01` as a cross-check collection.** Capturing NOAA's own daily product alongside
   `hourly02` would let silver compare against it. Not needed so far.

## References

- `packages/soil/src/grecohome_soil/`: `fetch.py`, `capture.py`, `dagster/`
- `docs/DEPLOYMENT.md`: Soil container and environment (`USCRN_*`)
- NOAA USCRN `hourly02` product and its `HEADERS.txt` (field order: `WBANNO`, `UTC_DATE`,
  `UTC_TIME`, …)
