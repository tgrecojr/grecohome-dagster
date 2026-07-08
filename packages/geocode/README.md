# grecohome-geocode

Bronze-only Dagster code location that builds a **reverse-geocode cache** for the
`location` bronze streams, using a self-hosted [Photon](https://github.com/komoot/photon)
service (`tuszik/photon-docker`).

## What it does

- Reads the `location` bronze points (Overland + OwnTracks) over a trailing window and
  snaps each to a ~11 m **grid cell** (`round(coord, 4)`, keyed as integer
  `(lat_e4, lon_e4)`).
- For each cell not already cached, calls Photon `GET {PHOTON_BASE_URL}/reverse` and
  captures the **raw** GeoJSON `FeatureCollection` to bronze (`geocode/reverse`,
  `capture_mode=raw`, content-hash deduped). The sidecar records the cell key
  (`lat_e4`/`lon_e4`) + the exact query point.
- The cache is the **source of truth** for lookups: `silver_location` joins location
  points to it with a pure offline DuckDB read (no network at transform time), so silver
  rebuilds stay cheap and don't re-hit Photon.

Idempotency needs no state dir — a cell recorded in a sidecar is never re-queried, so the
cache is its own durable ledger. See `docs/GEOCODE.md` for the full design, deployment
(runtime uid 1000, `PHOTON_BASE_URL`), and how to re-process at a different resolution.

## Layout

```
src/grecohome_geocode/
  config.py     # GeocodeSettings (PHOTON_BASE_URL, scan window, caps)
  cells.py      # the (lat_e4, lon_e4) cell-key contract shared with silver_location
  fetch.py      # sync httpx call to Photon /reverse (tenacity retry)
  capture.py    # bronze adapter -> capture_bronze(..., ext="json")
  discover.py   # pure-stdlib cell discovery (observed - cached)
  geocode.py    # the run: discover -> look up -> cache
  dagster/      # assets, checks, schedules, definitions
```

## Local run

```
uv run dagster dev -m grecohome_geocode.dagster.definitions
```

Requires `BRONZE_ROOT` (with `location/**` already promoted) and `PHOTON_BASE_URL`.
