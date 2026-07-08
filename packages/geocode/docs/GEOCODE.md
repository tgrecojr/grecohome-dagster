# Geocode subject (Photon reverse-geocode cache)

Enriches the `location` bronze points with place context, using a self-hosted
[Photon](https://github.com/komoot/photon) reverse geocoder (`tuszik/photon-docker`). This
note records the design specific to this subject; the bronze contract itself is in
[../../docs/BRONZE.md](../../docs/BRONZE.md), and the enriched table is `silver_location`
(see [../../docs/SILVER.md](../../docs/SILVER.md)).

## Why a bronze cache (not inline enrichment)

Reverse geocoding is a **network call per coordinate**. Silver/gold are pure, offline,
cheap-to-rebuild DuckDB projections that make no network calls — so the Photon lookups can't
live in the `silver_location` transform (every rebuild would re-hammer Photon and fail
offline). Instead the Photon responses become their own **bronze cache**: immutable,
append-only, the source of truth for lookups. `silver_location` then joins points to the
cache with a pure filesystem read. Same reason bronze exists for every other source.

## Flow

```
location bronze (overland + owntracks points)
   │  discover: snap each point to a ~11 m grid cell (lat_e4, lon_e4); subtract cached cells
   ▼
Photon  GET {PHOTON_BASE_URL}/reverse?lat=..&lon=..&lang=en&radius=0.05   (self-hosted, no auth)
   ▼  capture raw GeoJSON FeatureCollection, byte-for-byte
BRONZE_ROOT/geocode/reverse/dt=YYYY-MM-DD/reverse_{fetched_ms}_{id}.json (+ .meta.json)
   │  silver_location LEFT JOINs points → cache on (lat_e4, lon_e4)   [pure DuckDB, offline]
   ▼
SILVER_ROOT/location/silver_location.parquet
```

## The cell key (the shared contract)

A coordinate is snapped to an integer index of 1e-4-degree cells (~11 m):
`lat_e4 = round(lat * 10000)`. The cache is keyed by `(lat_e4, lon_e4)`; `silver_location`
recomputes the *same* key in DuckDB (`CAST(round(coord * 10000) AS BIGINT)`) and joins on it.
`grecohome_geocode.cells.snap_e4` rounds **half away from zero** to match DuckDB's `round()`.

The raw Photon body has no notion of our grid, so the cell key is recorded in the **sidecar**
(`lat_e4`/`lon_e4`) alongside the exact query point. That sidecar key is both the cache's
idempotency key and `silver_location`'s join key.

## Bronze mapping

| Field | Value |
|---|---|
| `source` | `geocode` |
| `collection` | `reverse` |
| `dt` | UTC **lookup** date (when we queried Photon) — a cache has no event timeline |
| payload | the Photon GeoJSON `FeatureCollection`, **verbatim** |
| `ext` | `json` |
| `dedupe` | `False` — idempotency is cell-based, not content-based (see below) |
| `capture_mode` | `raw` |
| sidecar extras | `lat_e4`, `lon_e4`, `query_lat`, `query_lon`, `cell_precision`, `params_key`, `request_params` (`radius`, `limit`, `lang`) |

The Photon query uses `radius`, `limit`, and `distance_sort=true` (nearest first), mirroring
Dawarich's `Places::NearbySearch` (`radius=0.5 km`, `limit=10`). We store the **whole**
FeatureCollection raw (the nearest `PHOTON_LIMIT` candidates); `silver_location` flattens
`features[0]` (the nearest) but the rest stay in bronze for smarter attribution later.

**`dedupe=False` is deliberate.** Two *distinct* cells legitimately return identical
responses (e.g. both an empty `{"...","features":[]}` "no result"). Content-hash dedup would
drop the second and leave that cell un-cached — re-looked-up on every run, forever. Since
discovery already guarantees one lookup per new cell, each capture is a genuine new cell and
must land.

## Idempotency (params-aware)

No state dir. The "already cached" set is derived from the geocode bronze **sidecars**
themselves — a cell counts as cached only if a sidecar records its `(lat_e4, lon_e4)` **and**
a `params_key` matching the *current* lookup params (`r=<radius>;l=<limit>;lang=<lang>`),
scanned across all partitions. So:

- a cell cached under the current params is never re-queried (the cache is its own durable
  ledger), and
- **changing `PHOTON_RADIUS_KM`, `PHOTON_LIMIT`, or `PHOTON_LANGUAGE` re-looks-up every
  affected cell on the next run** — the old-params sidecars no longer count as done. The new
  captures land beside the old ones (`dedupe=False`), and `silver_location` picks the latest
  per cell, so silver automatically prefers the new answer.

Discovery each run looks up only `observed(trailing window) − cached-under-current-params(all history)`.

## Checks

- **Content health** (WARN) — payloads parse and carry data. An empty-result Photon response
  is valid and passes (it's a real, cacheable answer we don't want to re-query).
- **Schema drift** (ERROR) — the stable top-level shape `["features","type"]`; a change means
  Photon's API contract moved. Per-feature `properties` are polymorphic but sit *inside*
  `features`, so they don't perturb the top-level signature (no false positives).
- **Freshness / completeness are disabled** — a cache is event-driven (no new ~11 m cell for
  weeks is normal, not stale), so the API-polling cadence checks would false-alarm.

## Deployment

- Image builds `nonroot` like every subject; **run it as uid 1000 at runtime**
  (`user: "1000:998"`) so it can read the `location` bronze the promoter (also uid 1000)
  wrote. It reads `location/**` and writes `geocode/**` under `BRONZE_ROOT`.
- Only `PHOTON_BASE_URL` is required (no secret). See
  [../../docs/ENV_TEMPLATE.md](../../docs/ENV_TEMPLATE.md).
- Register the code location in the host `workspace.yaml`
  (`-m grecohome_geocode.dagster.definitions`), and add a `photon` single-slot pool so
  overlapping runs never double-look-up.

## Changing params / resolution / reprocessing

Bronze keeps the raw points and raw Photon responses, so nothing is lost.

- **Change the lookup params** (`PHOTON_RADIUS_KM` / `PHOTON_LIMIT` / `PHOTON_LANGUAGE`): just
  update the env and let the next run re-geocode — the params-aware cache treats every cell as
  new under the changed `params_key`. Widen `GEOCODE_SCAN_DAYS` for one run to sweep all
  history, then rebuild `silver_location`. (The old-params captures remain in bronze as an
  audit trail; silver prefers the newest per cell.)
- **Change the cell size** (`CELL_PRECISION` in `cells.py`): a code change — different cells
  are computed, so every cell is "new"; run the cache with a wide `GEOCODE_SCAN_DAYS`, then
  rebuild `silver_location`.
- **Clean slate** (start the cache over): `geocode/` is a *derived, rebuildable* cache, so
  deleting `BRONZE_ROOT/geocode/reverse/` is safe — the next run repopulates it from the
  location bronze. (Do **not** delete anything else under `BRONZE_ROOT` — the rest is the
  source of truth.)

## Deferrals (v1 non-goals)

- **Nearest-only enrichment.** `silver_location` flattens `features[0]` (the nearest match).
  The full candidate `FeatureCollection` is kept raw in bronze, so smarter attribution (prefer
  a POI over a house within X m, disambiguate strip-mall units) can be added later by
  re-deriving silver — **without** re-hitting Photon.
- **No gold marts yet** (time-at-place, home/away, daily travel) — a natural next layer.
