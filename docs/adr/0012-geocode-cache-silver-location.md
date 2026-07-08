# ADR 0012: Reverse-geocode enrichment — a bronze Photon cache + silver_location

## Status
Accepted. Supersedes the "no `silver_location` in v1" deferral in the location subject
([packages/location/docs/LOCATION.md](../../packages/location/docs/LOCATION.md)).

## Context
The `location` bronze streams (Overland + OwnTracks) are raw coordinate fixes. Enriching them
with human place context (address, city, POI/OSM class) means **reverse geocoding**, for which
we self-host Photon (`tuszik/photon-docker`, upstream `komoot/photon`, OpenStreetMap data).

The tension: silver/gold are **pure, offline, cheap-to-rebuild DuckDB projections** that make no
network calls and are fully overwritten each run. Reverse geocoding is a **network call per
coordinate**. Baking Photon calls into the `silver_location` transform would re-hammer Photon on
every rebuild and fail whenever Photon is down — breaking the "rebuildable projection" property.

## Decision
Split enrichment into three pieces so silver stays a pure offline projection:

- **A new `geocode` bronze subject** (`packages/geocode`, `grecohome_geocode`) — a *derived* bronze
  **cache**, the source of truth for lookups. It reads the `location` bronze points, snaps each to
  a **~11 m grid cell** (`lat_e4 = round(lat * 10000)`), and for any not-yet-cached cell calls
  Photon `/reverse`, caching the raw GeoJSON `FeatureCollection` to `geocode/reverse` bronze. Photon
  is self-hosted/auth-less (`PHOTON_BASE_URL`, no secret). It's a distinct code location (not folded
  into `location`) so the promoter stays promote-only / no-source-API.
- **Cell-based idempotency, `dedupe=False`, no state dir.** Two *distinct* cells legitimately return
  identical responses (e.g. both an empty `features:[]` "no result"); content-hash dedup would drop
  the second and leave that cell re-looked-up forever. The cell key `(lat_e4, lon_e4)` is recorded
  in each **sidecar**, so the cache is its own durable ledger — discovery skips cells already in a
  sidecar and looks up only `observed(trailing window) − cached(all history)`.
- **`silver_location`** — a pure DuckDB projection: normalize both streams into one typed table
  (one row per fix, deduped on `(source_stream, event_ts_utc, lat, lon)`), snap to the **same** cell
  key, and LEFT JOIN the geocode cache on `(lat_e4, lon_e4)`, flattening the nearest match
  (`features[0]`) into `geo_*` columns + a `geocoded` flag. **No network at transform time.**
- **Shared cell-key contract.** `grecohome_geocode.cells.snap_e4` rounds **half away from zero** to
  match DuckDB's `round()`, so the Python (capture) and SQL (silver) keys agree exactly. Rounding
  (not geohash) is used precisely because it is expressible identically in both.
- **Runtime uid 1000.** Both `location` and `geocode` images build `nonroot` but run as uid 1000 so
  geocode can read the location bronze the promoter wrote.

## Consequences
- Photon lookups are cached immutably in bronze; `silver_location` rebuilds are cheap and offline,
  and re-derivable at a different resolution (bump `CELL_PRECISION`, backfill the cache once with a
  wide `GEOCODE_SCAN_DAYS`, rebuild silver) — bronze keeps the raw points and raw responses.
- A sixth published image (`grecohome-dagster-geocode`) wired into the CI matrix, GHCR retention,
  and `workspace.yaml`; a `photon` and a `geocode` single-slot concurrency pool.
- **Nearest-only enrichment in v1** (`features[0]`); the full candidate collection stays raw in
  bronze, so smarter POI attribution can be added later by re-deriving silver without re-hitting
  Photon.
- **Gold place marts** (time-at-place, home vs away, daily travel) remain deferred.

## Related
[[0001-bronze-only]] (derived-but-immutable cache), [[0008-silver-glucose]] (single-source silver
grain + dedup). Guides: `packages/geocode/docs/GEOCODE.md`, `docs/SILVER.md` (Location section).
