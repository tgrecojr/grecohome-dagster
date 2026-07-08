# grecohome-dagster

## Overview

A monorepo of personal data pipelines orchestrated by self-hosted Dagster, in three layers:

- **Bronze** — per-source code locations (Whoop, Garmin, Lingo, Soil/USCRN, Location, Geocode) that
  capture raw source responses to a bronze layer (the immutable source of truth). Geocode is a
  *derived* bronze cache: it reverse-geocodes the Location points via self-hosted Photon and caches
  the raw responses (append-only, immutable) so Silver can enrich offline.
- **Silver** — one cross-subject code location of derived, typed, deduplicated Parquet
  (sleep, glucose, workouts, recovery, location), read from bronze.
- **Gold** — one code location of analysis marts (daily wellness), read from silver.

Each layer = its own gRPC code-location image. Silver/gold are *derived and rebuildable* (they
can be dropped and regenerated); bronze is the only source of truth.

## Tech Stack

- Language: Python 3.14 (managed via uv; single `.python-version`)
- Packaging: uv workspace — one root `pyproject.toml`, one `uv.lock`, member packages under `packages/`
- Orchestration: Dagster (self-hosted; daemon + webserver run on the host)
- Bronze capture: httpx; OAuth: authlib; retries: tenacity; config: pydantic-settings; logging: structlog
- Silver/gold transforms: DuckDB over Parquet (no transactional database)

## Commands

- `uv sync --frozen` — install the workspace from the lock
- `uv run ruff check` — lint
- `uv run pytest` — run all tests
- `uv run dagster dev -m grecohome_whoop.dagster.definitions` — load the Whoop code location locally
  (swap the module for `grecohome_garmin` / `grecohome_lingo` / `grecohome_soil` /
  `grecohome_silver` / `grecohome_gold`)

## Architecture

- `packages/core` (`grecohome_core`) — shared, source-agnostic framework: bronze capture
  (atomic, append-only, content-hash dedup), sliding-window rate limiter, plaintext-JSON
  token file store, `BaseSubjectSettings`, logging, Dagster helpers (partitions, trailing
  window keys, async bridge), bronze asset-check builders, and the **silver helpers**
  (`grecohome_core.silver`: DuckDB connection, sidecar-safe bronze reading, dedup idiom,
  atomic Parquet write guarded by `protected_root`).
- **Bronze subjects** — `packages/{whoop,garmin,lingo,soil,location,geocode}`. Each is a code
  location that captures one source to bronze: Whoop (OAuth, hourly trailing-window + dedup), Garmin
  (delegated auth, daily capture-once, no dedup), Lingo (Drive service-account, sensor +
  dynamic partitions), Soil/USCRN (public file, daily row-slice + dedup), Location (no source API —
  *promotes* the external `locationrelay` service's raw staging files (Overland + OwnTracks POST
  bodies) into bronze byte-for-byte on a few-minute schedule; `dt`=receipt date, `dedupe`-off,
  idempotent via a per-stream promoted-set keyed on the staging **filename** + a `staging_file`
  sidecar backstop), Geocode (no external source — reverse-geocodes the Location points via
  self-hosted **Photon** `/reverse` and caches the raw GeoJSON to `geocode/reverse` bronze;
  `dedupe`-off, idempotent via a per-cell key `(lat_e4,lon_e4)` recorded in each sidecar; no state
  dir; `deps` on the two Location bronze assets). See `packages/geocode/docs/GEOCODE.md`.
- `packages/silver` (`grecohome_silver`) — one cross-subject code location: typed, deduped
  Parquet tables — `silver_sleep` (Garmin+Whoop FULL OUTER JOIN), `silver_glucose`,
  `silver_workouts`, `silver_recovery`, `silver_location` (Location points LEFT JOINed to the
  geocode cache on the ~11 m cell key — a pure offline enrichment join), and the other
  derived tables — plus their asset checks. Reads `BRONZE_ROOT`, writes `SILVER_ROOT`.
- `packages/gold` (`grecohome_gold`) — analysis marts: `gold_daily_wellness` (one row per
  local day joining the four silver tables). Reads `SILVER_ROOT`, writes `GOLD_ROOT`.
- Cross-layer lineage is declared by `AssetKey` (silver→bronze, gold→silver); the reads are
  **filesystem reads** of the upstream root, not gRPC calls. Each layer ships its own gRPC
  code-location image; the host registers them via `workspace.yaml`. We ship code locations
  only — never a Dagster instance/webserver. See `docs/SILVER.md`, `docs/GOLD.md`.

## Hard constraints

- **Pins:** `dagster==1.13.10` and all `dagster-*==0.29.10` (must match the host so the
  daemon ↔ code-location gRPC protocol stays in sync). Other libs pinned `==`; pure-data
  deps (`tzdata`) use `>=`. DuckDB (silver/gold) is pinned `==`.
- **No transactional database.** Bronze is files; silver/gold are Parquet via DuckDB. No
  Postgres, no SQLAlchemy, no Alembic, no APScheduler. Don't reintroduce them.
- **Layers are append-only / rebuildable.** Bronze is immutable raw capture; silver/gold
  fully overwrite their Parquet each run (a pure projection of the layer below) and must
  **never write under the layer below** (the `protected_root` guard enforces it). Bronze
  stays the only source of truth.
- **Single user** (user_id=1). Don't add multi-user/tenant scaling. Partition by date only.
- **Tokens:** plaintext JSON at `WHOOP_TOKEN_PATH`, rewritten atomically (Whoop rotates the
  refresh token every refresh). No Fernet/`TOKEN_ENCRYPTION_KEY`.
- **Bronze partitions are UTC fetch-slices, not local days.** Local-day / event-date
  semantics live in silver (e.g. sleep night = local wake date), applied at read time over
  bronze's raw UTC timestamps — never by trusting the partition folder.
- **Location = promote-only, single-writer lake.** The internet-facing `locationrelay` (separate
  Rust repo/container) stages raw POST bodies; the `location` subject only *promotes* them to
  bronze — it makes **no source-API calls** and holds no secret. Python stays the lake's sole
  writer: the promoter never writes/deletes under `RELAY_CAPTURE_DIR` (relay is its sole cleaner),
  and the relay never writes the lake. Don't add a Dagster→Dawarich forwarder (the relay forwards
  in real time). Location enrichment lives in the **separate `geocode` subject**, not here, so the
  promoter stays pure.
- **Geocode = derived bronze cache, offline enrichment.** Reverse geocoding is a per-coordinate
  network call, so it must NOT live in the `silver_location` transform (silver is a pure, offline,
  cheap-to-rebuild projection). The `geocode` subject caches Photon `/reverse` responses to bronze
  (append-only, `dedupe`-off, cell-keyed idempotency in the sidecar); `silver_location` then joins
  points to that cache with a pure DuckDB filesystem read. Photon is self-hosted/auth-less
  (`PHOTON_BASE_URL`, no secret). The v1 "no `silver_location`" deferral is **superseded** by this
  work — `silver_location` and `geocode` now exist; gold place marts (time-at-place, home/away)
  remain deferred.

## Environment Variables

See `docs/ENV_TEMPLATE.md` (and `.env.example`). Per layer:
- **bronze subjects** — `BRONZE_ROOT` (+ per-subject auth: e.g. Whoop `WHOOP_CLIENT_ID` /
  `WHOOP_CLIENT_SECRET` / `WHOOP_TOKEN_PATH`; Garmin `GARMINCONNECT_*`; Lingo `GDRIVE_*`;
  Soil `USCRN_*`; Location `RELAY_CAPTURE_DIR` (mount **read-only**) / `LOCATION_STATE_DIR`
  (outside `BRONZE_ROOT`) — no secret; image builds as `nonroot` like the others but must run **at
  runtime as uid 1000** (e.g. `user: "1000:998"`) to read the `0600` staging files; Geocode
  `PHOTON_BASE_URL` — no secret, no state dir; also runs **at runtime as uid 1000** to read the
  Location bronze it enriches).
- **silver** — `BRONZE_ROOT` (read-only), `SILVER_ROOT` (+ reserved `SILVER_MONITOR_DIR`).
- **gold** — `SILVER_ROOT` (read-only), `GOLD_ROOT` (+ reserved `GOLD_MONITOR_DIR`).
