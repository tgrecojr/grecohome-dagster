# grecohome-dagster

A monorepo of personal health/environment data pipelines, orchestrated by self-hosted
[Dagster](https://dagster.io/), in three layers:

- **Bronze** — raw source data, the immutable source of truth. Soil/USCRN is captured here
  by its own code location; Whoop and Garmin are captured by
  [WearStream](https://github.com/chainringlabs/wearstream), an external service that writes
  the same bronze layout into the same tree.
- **Silver** — one cross-subject code location of typed, deduplicated **Parquet** (sleep,
  workouts, recovery, daily, weather, …), derived from bronze.
- **Gold** — analysis marts (daily wellness) derived from silver.

Silver and gold are derived and fully rebuildable (DuckDB over Parquet — no database); bronze
stays the only source of truth.

## Why a monorepo

- One place to manage the Python toolchain and shared dependencies.
- Shared, reusable components (`grecohome-core`) instead of re-implementing capture, checks,
  silver helpers and Dagster plumbing per layer.
- Each subject still deploys independently as its own gRPC **code-location image**.

## Layout

```
packages/
  core/    grecohome-core   — shared framework (bronze capture + checks, silver helpers, dagster plumbing)
  soil/    grecohome-soil   — NOAA USCRN soil/temp bronze subject (ported from soildata)
  silver/  grecohome-silver — silver layer (sleep, workouts, recovery, daily, weather, …)
  gold/    grecohome-gold   — gold layer (daily wellness and daily weather marts)
docs/      high-level-design, ARCHITECTURE, DEPLOYMENT, ENV_TEMPLATE, intent/
```

## Docs

- [Architecture](docs/ARCHITECTURE.md) — repo shape, core vs subject, orchestration model
- [Bronze writer](docs/intent/core/bronze/bronze-design.md) — capture rules, layout, sidecar, skip-identical
- [Bronze checks](docs/intent/core/checks/checks-design.md) — the four check families, alerting, checks-only jobs
- [Silver layer](docs/intent/silver/silver-design.md) — derived/rebuildable Parquet: sleep, recovery + strain, workouts, weather, daily, body, fitness
- [Gold layer](docs/intent/gold/gold-design.md) — analysis marts from silver (daily wellness, daily weather) and the run monitors
- [Deployment](docs/DEPLOYMENT.md) — host `workspace.yaml`, mounts, concurrency pool, what to enable

This is a [uv workspace](https://docs.astral.sh/uv/concepts/projects/workspaces/): one
root `pyproject.toml`, one `uv.lock`, one managed Python version.

## Architecture

- **Three layers.** Bronze subjects call their source API and write raw payloads to
  `BRONZE_ROOT` (atomic, append-only, content-hash dedup); WearStream writes Whoop and Garmin
  into the same tree with the same layout. **Silver** reads bronze and writes typed, deduplicated Parquet to
  `SILVER_ROOT`; **gold** reads silver and writes analysis marts to `GOLD_ROOT`. Silver/gold
  are DuckDB-over-Parquet, fully rebuildable, and never write under the layer below. See
  [`docs/intent/silver/`](docs/intent/silver/silver-design.md) and [`docs/intent/gold/`](docs/intent/gold/gold-design.md).
- **Self-hosted Dagster.** The daemon + webserver run on the host. Each layer (the bronze
  subject, silver, gold) ships a gRPC code-location image that registers with the host via
  `workspace.yaml`. Dagster libraries are pinned (`dagster==1.13.30`, `dagster-*==0.29.30`) to
  match the host so the daemon ↔ code-location gRPC contract stays in sync.
- **Cross-layer lineage by `AssetKey`.** silver→bronze and gold→silver deps render in the UI;
  the reads themselves are filesystem reads of the upstream root, not gRPC calls.
- **Orchestration.** Soil/USCRN: daily UTC partitions where
  each stores only that day's rows sliced from the public NOAA year file, re-captured every 6h
  with dedup. Backfill (where applicable) via `dagster backfill`. **Silver/gold** rebuild
  whole-table on daily schedules (silver ~06:00–06:50 UTC, gold 07:30) — each a pure projection
  of the layer below, so a rebuild is idempotent.

## Commands

```bash
uv sync --frozen          # install the workspace from the lock
uv run ruff check         # lint
uv run pytest             # run all package tests
uv run dagster dev -m grecohome_soil.dagster.definitions    # load the Soil/USCRN code location locally
```

## Environment variables

See [`docs/ENV_TEMPLATE.md`](docs/ENV_TEMPLATE.md). Copy the template to `.env` for local
dev; production injects values via Ansible from a secrets manager.

## Deployment

One code-location image per layer published to GHCR
(`ghcr.io/tgrecojr/grecohome-dagster-<name>` for
`soil`/`silver`/`gold`).
See [`docs/DEPLOYMENT.md`](docs/DEPLOYMENT.md) for the host `workspace.yaml`, mounts (silver reads
bronze read-only; gold reads silver read-only), and concurrency-pool wiring.

## Contributing

Issues and pull requests are welcome. Please read [CONTRIBUTING.md](CONTRIBUTING.md) first.

## License

Released under the [MIT License](LICENSE).
