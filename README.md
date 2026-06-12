# grecohome-dagster

A monorepo of personal health/environment data pipelines, orchestrated by self-hosted
[Dagster](https://dagster.io/), in three layers:

- **Bronze** — per-source code locations (Whoop, Garmin, Lingo, Soil/USCRN) capture raw source
  data to a bronze layer (the immutable source of truth).
- **Silver** — one cross-subject code location of typed, deduplicated **Parquet** (sleep,
  glucose, workouts, recovery), derived from bronze.
- **Gold** — analysis marts (daily wellness) derived from silver.

Silver and gold are derived and fully rebuildable (DuckDB over Parquet — no database); bronze
stays the only source of truth.

## Why a monorepo

- One place to manage the Python toolchain and shared dependencies.
- Shared, reusable components (`grecohome-core`) instead of re-implementing capture, rate
  limiting, token handling, and Dagster plumbing per subject.
- Each subject still deploys independently as its own gRPC **code-location image**.

## Layout

```
packages/
  core/    grecohome-core   — shared framework (bronze capture + checks, silver helpers, dagster plumbing)
  whoop/   grecohome-whoop  — Whoop bronze subject (migrated from whoopster)
  garmin/  grecohome-garmin — Garmin bronze subject (ported from garmincapture)
  lingo/   grecohome-lingo  — Lingo CGM bronze subject (ported from glucose-loader)
  soil/    grecohome-soil   — NOAA USCRN soil/temp bronze subject (ported from soildata)
  silver/  grecohome-silver — silver layer (sleep, glucose, workouts, recovery)
  gold/    grecohome-gold   — gold layer (daily wellness mart)
docs/      ARCHITECTURE, BRONZE, SILVER, GOLD, DEPLOYMENT, ENV_TEMPLATE, VALIDATION, adr/
```

## Docs

- [Architecture](docs/ARCHITECTURE.md) — repo shape, core vs subject, orchestration model
- [Bronze layer](docs/BRONZE.md) — capture invariants, layout, sidecar
- [Silver layer](docs/SILVER.md) — derived/rebuildable Parquet; sleep + glucose + workouts + recovery
- [Gold layer](docs/GOLD.md) — analysis marts from silver; daily wellness mart (sleep + recovery + workouts + glucose)
- [Deployment](docs/DEPLOYMENT.md) — host `workspace.yaml`, concurrency pool, OAuth / service-account setup
- [ADRs](docs/adr/) — bronze-only, Dagster pins, token file, Garmin/Lingo/Soil ports, silver sleep/glucose/workouts/recovery, gold wellness

This is a [uv workspace](https://docs.astral.sh/uv/concepts/projects/workspaces/): one
root `pyproject.toml`, one `uv.lock`, one managed Python version.

## Architecture

- **Three layers.** Bronze subjects call their source API and write raw payloads to
  `BRONZE_ROOT` (atomic, append-only; content-hash dedup opt-in — on for Whoop, off for
  immutable Garmin). **Silver** reads bronze and writes typed, deduplicated Parquet to
  `SILVER_ROOT`; **gold** reads silver and writes analysis marts to `GOLD_ROOT`. Silver/gold
  are DuckDB-over-Parquet, fully rebuildable, and never write under the layer below. See
  [`docs/SILVER.md`](docs/SILVER.md) and [`docs/GOLD.md`](docs/GOLD.md).
- **Self-hosted Dagster.** The daemon + webserver run on the host. Each layer (every bronze
  subject, silver, gold) ships a gRPC code-location image that registers with the host via
  `workspace.yaml`. Dagster libraries are pinned (`dagster==1.13.8`, `dagster-*==0.29.8`) to
  match the host so the daemon ↔ code-location gRPC contract stays in sync.
- **Cross-layer lineage by `AssetKey`.** silver→bronze and gold→silver deps render in the UI;
  the reads themselves are filesystem reads of the upstream root, not gRPC calls.
- **Orchestration varies by source.** Whoop: daily UTC partitions, one hourly schedule
  re-captures the trailing 8 (rescores) + content-hash dedup. Garmin: daily capture-once over
  the trailing window (immutable, no dedup). Lingo: a Drive **sensor** + dynamic partitions
  keyed on file id (file-arrival-driven, no schedule). Soil/USCRN: daily UTC partitions where
  each stores only that day's rows sliced from the public NOAA year file, re-captured every 6h
  with dedup. Backfill (where applicable) via `dagster backfill`. **Silver/gold** rebuild
  whole-table on daily schedules (silver ~06:00–06:50 UTC, gold 07:30) — each a pure projection
  of the layer below, so a rebuild is idempotent.

## Commands

```bash
uv sync --frozen          # install the workspace from the lock
uv run ruff check         # lint
uv run pytest             # run all package tests
uv run dagster dev -m grecohome_whoop.dagster.definitions   # load the Whoop code location locally
```

## Environment variables

See [`docs/ENV_TEMPLATE.md`](docs/ENV_TEMPLATE.md). Copy the template to `.env` for local
dev; production injects values via Ansible from a secrets manager.

## Deployment

One code-location image per layer published to GHCR
(`ghcr.io/tgrecojr/grecohome-dagster-<name>` for `whoop`/`garmin`/`lingo`/`soil`/`silver`/`gold`).
See [`docs/DEPLOYMENT.md`](docs/DEPLOYMENT.md) for the host `workspace.yaml`, mounts (silver reads
bronze read-only; gold reads silver read-only), and concurrency-pool wiring.
