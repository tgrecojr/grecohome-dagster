# Architecture

`grecohome-dagster` is a monorepo of personal **data-subject** pipelines orchestrated by
self-hosted Dagster. Each subject is a **bronze-only** code location: it calls a source API
and writes the raw responses to a bronze layer. No transformation, no database — silver/gold
are a future phase.

## Repository shape (uv workspace)

```
packages/
  core/    grecohome-core  — shared, source-agnostic framework
  whoop/   grecohome-whoop — the Whoop data subject (migrated from the old whoopster app)
  garmin/  scaffold (README only)
  lingo/   scaffold (README only)
```

One root `pyproject.toml`, one `uv.lock`, one pinned Python (`.python-version`). Subjects
depend on `grecohome-core` via the workspace. See [ADRs](adr/) for the load-bearing decisions.

## `grecohome-core` — the shared framework

| Module | Responsibility |
|---|---|
| `bronze/capture.py` | Atomic, append-only raw-payload capture with content-hash dedup |
| `http/rate_limiter.py` | In-process sliding-window API rate limiter (within-run guard) |
| `tokens/file_store.py` | Atomic plaintext-JSON token store (temp + `os.replace`) |
| `config.py` | `BaseSubjectSettings` + `init_settings()` (friendly missing-var errors) |
| `logging_config.py` | structlog / JSON logging setup |
| `dagster/helpers.py` | `daily_utc_partitions`, `trailing_partition_keys`, `run_async` |

Nothing in core is Whoop-specific; a new subject reuses all of it.

## `grecohome-whoop` — a data subject

```
grecohome_whoop/
  config.py          WhoopSettings(BaseSubjectSettings)
  auth/
    oauth_client.py  OAuth 2.0 + PKCE (authorize, exchange, refresh)
    token_manager.py token lifecycle over the core file store (rotation-safe)
  api/
    whoop_client.py  async API client; captures every response to bronze
  dagster/
    assets.py        daily-partitioned bronze assets + snapshots asset
    schedules.py     hourly schedules + asset jobs
    definitions.py   the gRPC code-location target
  oauth_setup.py     one-time interactive OAuth (browser / headless)
```

### Data flow (per run)

```
Dagster schedule (hourly, UTC)
  └─ RunRequest(partition_key=YYYY-MM-DD)
       └─ bronze asset (pool: whoop_api)
            └─ WhoopClient(bronze_dt=partition).get_<collection>(start, end)
                 └─ _make_request → capture_bronze(raw bytes + sidecar) → BRONZE_ROOT
```

The asset does no persistence beyond bronze; record counts are attached as run metadata.

## Orchestration model

- **Self-hosted Dagster.** The daemon + webserver run on the host. Each subject ships a gRPC
  **code-location image** (`dagster code-server start`) that the host registers via
  `workspace.yaml`. We never ship a Dagster instance/webserver. See [DEPLOYMENT](DEPLOYMENT.md).
- **Daily UTC partitions** (`end_offset=1` so the in-progress day is materializable). A partition
  is a UTC *fetch-slice*, not a semantic local day — see [ADR: bronze partitioning](adr/0001-bronze-only.md).
- **One hourly schedule** re-materializes the trailing `reconcile_window_days + 1` partitions, so
  Whoop's retroactive rescores/deletes are eventually re-captured. Correctness comes from the
  trailing window + content-hash dedup, not the cadence.
- **One shared `whoop_api` concurrency pool** (limit enforced on the host) bounds total API
  usage across the tick + any backfill and serializes token access.
- **Backfill** = `dagster backfill` over the same assets.

## Per-subject deployment

Each subject is built into its own image (`ghcr.io/tgrecojr/grecohome-dagster-<subject>`) by a
CI matrix, signed (cosign) with an SBOM + SLSA provenance. Subjects deploy, fail, and scale
independently.
