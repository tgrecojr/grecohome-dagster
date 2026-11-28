# grecohome-dagster

A monorepo of personal health data pipelines, orchestrated by self-hosted
[Dagster](https://dagster.io/), in three layers:

- **Bronze** — raw source data, the immutable source of truth. Whoop and Garmin are captured
  by [WearStream](https://github.com/chainringlabs/wearstream), an external service that
  writes the bronze tree; this repo only reads it.
- **Silver** — one cross-subject code location of typed, deduplicated **Parquet** (sleep,
  workouts, recovery, daily, …), derived from bronze.
- **Gold** — analysis marts (daily wellness) derived from silver.

Silver and gold are derived and fully rebuildable (DuckDB over Parquet — no database); bronze
stays the only source of truth.

## Why a monorepo

- One place to manage the Python toolchain and shared dependencies.
- Shared, reusable components (`grecohome-core`) instead of re-implementing checks and
  silver helpers per layer.
- Each layer still deploys independently as its own gRPC **code-location image**.

## Layout

```
packages/
  core/    grecohome-core   — shared library (check helpers, silver helpers)
  silver/  grecohome-silver — silver layer (sleep, workouts, recovery, daily, …)
  gold/    grecohome-gold   — gold layer (daily wellness mart, run monitors)
docs/      high-level-design, ARCHITECTURE, DEPLOYMENT, ENV_TEMPLATE, intent/
```

## Docs

- [High-level design](docs/high-level-design.md) — layers, the bronze read contract, key decisions
- [Architecture](docs/ARCHITECTURE.md) — repo shape, core vs layer, orchestration model
- [Check helpers](docs/intent/core/checks/checks-design.md) — alerting, checks-only jobs, table freshness
- [Silver layer](docs/intent/silver/silver-design.md) — derived/rebuildable Parquet: sleep, recovery + strain, workouts, daily, body, fitness
- [Gold layer](docs/intent/gold/gold-design.md) — analysis marts from silver (daily wellness) and the run monitors
- [Deployment](docs/DEPLOYMENT.md) — host `workspace.yaml`, mounts, what to enable

This is a [uv workspace](https://docs.astral.sh/uv/concepts/projects/workspaces/): one
root `pyproject.toml`, one `uv.lock`, one managed Python version.

## Architecture

- **Three layers.** WearStream calls the Whoop and Garmin APIs and writes raw payloads to
  `BRONZE_ROOT` (atomic, append-only, content-hash dedup). **Silver** reads bronze and writes typed, deduplicated Parquet to
  `SILVER_ROOT`; **gold** reads silver and writes analysis marts to `GOLD_ROOT`. Silver/gold
  are DuckDB-over-Parquet, fully rebuildable, and never write under the layer below. See
  [`docs/intent/silver/`](docs/intent/silver/silver-design.md) and [`docs/intent/gold/`](docs/intent/gold/gold-design.md).
- **Self-hosted Dagster.** The daemon + webserver run on the host. Each layer (silver,
  gold) ships a gRPC code-location image that registers with the host via
  `workspace.yaml`. Dagster libraries are pinned (`dagster==1.13.31`, `dagster-*==0.29.31`) to
  match the host so the daemon ↔ code-location gRPC contract stays in sync.
- **Cross-layer lineage by `AssetKey`.** silver→bronze and gold→silver deps render in the UI;
  the reads themselves are filesystem reads of the upstream root, not gRPC calls.
- **Orchestration.** Silver and gold rebuild whole-table on daily schedules (silver
  ~06:00–06:55 UTC, gold 07:30) — each a pure projection
  of the layer below, so a rebuild is idempotent.

## Commands

```bash
uv sync --frozen          # install the workspace from the lock
uv run ruff check         # lint
uv run pytest             # run all package tests
uv run dagster dev -m grecohome_silver.dagster.definitions  # load the silver code location locally
```

## Environment variables

See [`docs/ENV_TEMPLATE.md`](docs/ENV_TEMPLATE.md). Copy the template to `.env` for local
dev; production injects values via Ansible from a secrets manager.

## Deployment

One code-location image per layer published to GHCR
(`ghcr.io/tgrecojr/grecohome-dagster-<name>` for `silver`/`gold`).
See [`docs/DEPLOYMENT.md`](docs/DEPLOYMENT.md) for the host `workspace.yaml` and mounts (silver
reads bronze read-only; gold reads silver read-only).

## Contributing

Issues and pull requests are welcome. Please read [CONTRIBUTING.md](CONTRIBUTING.md) first.

## License

Released under the [MIT License](LICENSE).
