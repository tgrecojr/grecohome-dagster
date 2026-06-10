# ADR 0006: Soil/USCRN port — daily row-slice + dedup over a growing year file

## Status
Accepted.

## Context
Soil (NOAA USCRN soil/temperature) is the fourth data subject, ported from the standalone
`soildata` service (Rust + Postgres). The source is unlike the others: a **public NOAA file**
(`hourly02` product, no auth) — one headerless, whitespace-delimited file **per station-year**
(`CRNH0203-{year}-{station}.txt`) that gains **one row per hour**. `soildata` re-downloaded the whole
current-year file hourly and upserted rows into Postgres keyed on `(wbanno, utc_datetime)`; its
optional bronze layer stored the **entire file** on every fetch. We need the raw rows a few times a
day, bronze-only, **without re-storing the whole year** each tick.

## Decision
- **Reuse `grecohome-core`** for capture, settings, and logging. Soil-specific logic (`fetch`,
  `capture`) lives in `packages/soil`. No Postgres, no typed-column parsing, no `garminconnect`-style
  client — just `httpx` for one public GET.
- **Storage = daily UTC partition, payload = that day's rows.** Each partition fetches the year file
  and stores only the lines whose `UTC_DATE` (field 2) matches the partition date, content-hash
  **deduped**. A whole-file hash would change every hour and never dedup; a per-day slice means a
  finished day stores **once** (~24 lines) and today re-writes only when a new row appears. The
  stored lines are byte-faithful to the source (selection, not transformation), so bronze stays raw.
- **No auth, no credential mount.** The simplest subject to deploy — two mounts (bronze + the shared
  `dagster.yaml`), no token/key.
- **Schedule-driven** (like Whoop/Garmin): `uscrn_schedule` re-captures the trailing
  `USCRN_LOOKBACK_DAYS` daily partitions every 6h, `run_key` carries the tick so re-emits are
  distinct runs and content-hash dedup keeps storage flat. Full history is reachable via
  `dagster backfill` (every past year is on the server); a 404 for an absent year/station is a
  graceful skip so wide backfills stay robust.
- **Single station** (env-configurable, default `PA_Avondale_2_N`), recorded in the sidecar, not the
  path — consistent with the single-user/single-station constraint.

## Consequences
- The bronze layout gains `uscrn/hourly/dt=<UTC date>/...txt`. Today's folder may hold a few
  progressively-larger day-slices (each a faithful snapshot at capture time); a completed day
  stabilizes to one file via dedup.
- Bandwidth: each tick still GETs the whole year file (the source offers no incremental endpoint),
  but **storage** scales with the data, not with fetch frequency — which was the goal.
- No core change was needed; `capture_bronze` already supports `dedupe` / explicit `ext` / `dt` /
  `meta` passthrough (proven by the Lingo port).

## Related
[[0001-bronze-only]], [[0002-dagster-pins]], [[0004-garmin-port]], [[0005-lingo-port]].
