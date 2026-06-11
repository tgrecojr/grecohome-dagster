# grecohome-dagster

A monorepo of personal **data-subject** pipelines, orchestrated by self-hosted
[Dagster](https://dagster.io/). Each data subject (Whoop, Garmin, Lingo, Soil/USCRN) is a
**bronze-only** code location that captures raw source data to a bronze layer — no
transformation, no database. Silver/gold are a future phase.

## Why a monorepo

- One place to manage the Python toolchain and shared dependencies.
- Shared, reusable components (`grecohome-core`) instead of re-implementing capture, rate
  limiting, token handling, and Dagster plumbing per subject.
- Each subject still deploys independently as its own gRPC **code-location image**.

## Layout

```
packages/
  core/    grecohome-core   — shared framework (bronze capture, rate limiter, token store, dagster helpers)
  whoop/   grecohome-whoop  — Whoop data subject (migrated from whoopster)
  garmin/  grecohome-garmin — Garmin data subject (ported from garmincapture)
  lingo/   grecohome-lingo  — Lingo CGM data subject (ported from glucose-loader)
  soil/    grecohome-soil   — NOAA USCRN soil/temp data subject (ported from soildata)
docs/      ARCHITECTURE, BRONZE, DEPLOYMENT, ENV_TEMPLATE, adr/
```

## Docs

- [Architecture](docs/ARCHITECTURE.md) — repo shape, core vs subject, orchestration model
- [Bronze layer](docs/BRONZE.md) — capture invariants, layout, sidecar
- [Silver layer](docs/SILVER.md) — derived/rebuildable Parquet; sleep + glucose + workouts + recovery
- [Deployment](docs/DEPLOYMENT.md) — host `workspace.yaml`, concurrency pool, OAuth / service-account setup
- [ADRs](docs/adr/) — bronze-only, Dagster pins, token file, Garmin/Lingo/Soil ports, silver sleep/glucose/workouts/recovery

This is a [uv workspace](https://docs.astral.sh/uv/concepts/projects/workspaces/): one
root `pyproject.toml`, one `uv.lock`, one managed Python version.

## Architecture

- **Bronze-only.** Subjects call their source API and write raw payloads to `BRONZE_ROOT`
  (atomic, append-only; content-hash dedup is opt-in — on for Whoop, off for immutable Garmin).
  Downstream silver/gold reads bronze later.
- **Self-hosted Dagster.** The daemon + webserver run on the host. Each subject ships a
  gRPC code-location image that registers with the host via `workspace.yaml`. Dagster
  libraries are pinned (`dagster==1.13.8`, `dagster-*==0.29.8`) to match the host so the
  daemon ↔ code-location gRPC contract stays in sync.
- **Orchestration varies by source.** Whoop: daily UTC partitions, one hourly schedule
  re-captures the trailing 8 (rescores) + content-hash dedup. Garmin: daily capture-once over
  the trailing window (immutable, no dedup). Lingo: a Drive **sensor** + dynamic partitions
  keyed on file id (file-arrival-driven, no schedule). Soil/USCRN: daily UTC partitions where
  each stores only that day's rows sliced from the public NOAA year file, re-captured every 6h
  with dedup. Backfill (where applicable) via `dagster backfill`.

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

Per-subject code-location images published to GHCR
(`ghcr.io/tgrecojr/grecohome-dagster-<subject>`). See [`docs/DEPLOYMENT.md`](docs/DEPLOYMENT.md)
for the host `workspace.yaml` and concurrency-pool wiring.
