# grecohome-dagster

## Overview

A monorepo of personal data-subject pipelines orchestrated by self-hosted Dagster. Each
subject (Whoop now; Garmin, Lingo later) is a **bronze-only** Dagster code location that
captures raw source-API responses to a bronze layer. No Postgres, no silver in this phase.

## Tech Stack

- Language: Python 3.14 (managed via uv; single `.python-version`)
- Packaging: uv workspace — one root `pyproject.toml`, one `uv.lock`, member packages under `packages/`
- Orchestration: Dagster (self-hosted; daemon + webserver run on the host)
- HTTP: httpx; OAuth: authlib; retries: tenacity; config: pydantic-settings; logging: structlog

## Commands

- `uv sync --frozen` — install the workspace from the lock
- `uv run ruff check` — lint
- `uv run pytest` — run all tests
- `uv run dagster dev -m grecohome_whoop.dagster.definitions` — load the Whoop code location locally

## Architecture

- `packages/core` (`grecohome_core`) — shared, source-agnostic framework: bronze capture
  (atomic, append-only, content-hash dedup), sliding-window rate limiter, plaintext-JSON
  token file store, `BaseSubjectSettings`, logging, Dagster helpers (partitions, trailing
  window keys, async bridge).
- `packages/whoop` (`grecohome_whoop`) — Whoop subject: OAuth client, token manager (over
  the file store), Whoop API client (capture always-on), and the Dagster code location
  (daily UTC-partitioned bronze assets + hourly trailing-8 schedule).
- Each subject ships its own gRPC code-location image; the host registers them via
  `workspace.yaml`. We ship code locations only — never a Dagster instance/webserver.

## Hard constraints

- **Pins:** `dagster==1.13.8` and all `dagster-*==0.29.8` (must match the host so the
  daemon ↔ code-location gRPC protocol stays in sync). Other libs pinned `==`; pure-data
  deps (`tzdata`) use `>=`.
- **Bronze-only:** no Postgres, no SQLAlchemy, no Alembic, no APScheduler. Don't reintroduce them.
- **Single user** (user_id=1). Don't add multi-user/tenant scaling. Partition by date only.
- **Tokens:** plaintext JSON at `WHOOP_TOKEN_PATH`, rewritten atomically (Whoop rotates the
  refresh token every refresh). No Fernet/`TOKEN_ENCRYPTION_KEY`.
- **Bronze partitions are UTC fetch-slices, not local days.** Local-day semantics live in
  future silver/gold, applied at read time.

## Environment Variables

See `docs/ENV_TEMPLATE.md` (and `.env.example`). Required: `BRONZE_ROOT`, `WHOOP_CLIENT_ID`,
`WHOOP_CLIENT_SECRET`, `WHOOP_TOKEN_PATH`.
