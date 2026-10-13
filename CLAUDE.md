# grecohome-dagster

## Overview

A monorepo of personal data pipelines orchestrated by self-hosted Dagster, in three layers:

- **Bronze** — per-source code locations (Whoop, Garmin, Lingo, Soil/USCRN) that
  capture raw source responses to a bronze layer (the immutable source of truth).
- **Silver** — one cross-subject code location of derived, typed, deduplicated Parquet
  (sleep, glucose, workouts, recovery), read from bronze.
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
- **Bronze subjects** — `packages/{whoop,garmin,lingo,soil}`. Each is a code
  location that captures one source to bronze: Whoop (OAuth, hourly trailing-window + dedup), Garmin
  (delegated auth, daily trailing-window re-capture + skip-identical — see
  `docs/intent/garmin/capture/capture-design.md`), Lingo (Drive service-account, sensor +
  dynamic partitions), Soil/USCRN (public file, daily row-slice + dedup).
- `packages/silver` (`grecohome_silver`) — one cross-subject code location: typed, deduped
  Parquet tables — `silver_sleep` (Garmin+Whoop FULL OUTER JOIN), `silver_glucose`,
  `silver_workouts`, `silver_recovery`, and the other derived tables — plus their asset
  checks. Reads `BRONZE_ROOT`, writes `SILVER_ROOT`.
- `packages/gold` (`grecohome_gold`) — analysis marts: `gold_daily_wellness` (one row per
  local day joining the four silver tables). Reads `SILVER_ROOT`, writes `GOLD_ROOT`.
- Cross-layer lineage is declared by `AssetKey` (silver→bronze, gold→silver); the reads are
  **filesystem reads** of the upstream root, not gRPC calls. Each layer ships its own gRPC
  code-location image; the host registers them via `workspace.yaml`. We ship code locations
  only — never a Dagster instance/webserver. See `docs/SILVER.md`, `docs/GOLD.md`.

## Hard constraints

- **Pins:** `dagster==1.13.22` and all `dagster-*==0.29.22` (must match the host so the
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
- **Location/geocode are retired.** The `location` and `geocode` subjects, `silver_location`,
  and the planned gold place marts were removed in 2026-08 (the `locationrelay`/Dawarich/Photon
  stack lives outside this repo). Don't reintroduce location processing here.

## Environment Variables

See `docs/ENV_TEMPLATE.md` (and `.env.example`). Per layer:
- **bronze subjects** — `BRONZE_ROOT` (+ per-subject auth: e.g. Whoop `WHOOP_CLIENT_ID` /
  `WHOOP_CLIENT_SECRET` / `WHOOP_TOKEN_PATH`; Garmin `GARMINCONNECT_*`; Lingo `GDRIVE_*`;
  Soil `USCRN_*`).
- **silver** — `BRONZE_ROOT` (read-only), `SILVER_ROOT` (+ reserved `SILVER_MONITOR_DIR`).
- **gold** — `SILVER_ROOT` (read-only), `GOLD_ROOT` (+ reserved `GOLD_MONITOR_DIR`).

## LID
- Mode: Scoped
- Version: 1.3.0

## LID Scope

Paths in scope:
- `packages/garmin/**`
- `packages/whoop/**`
- `packages/soil/**`
- `packages/lingo/**`
- `packages/silver/**`
- `packages/core/src/grecohome_core/bronze/**`
- `packages/core/src/grecohome_core/checks/**`
- `packages/core/src/grecohome_core/silver/**`

## Linked-Intent Development (MANDATORY)

**Consult the `linked-intent-dev` skill for ALL code changes.** All changes flow through the arrow of intent in one direction:

```
HLD → LLDs → EARS → Tests → Code
```

- **New features and refactors**: full six-phase workflow (HLD check → LLD check/draft → EARS → intent-narrowing edge audit → tests-first → code).
- **Bug fixes**: walk the arrow like any other change — find where behavior diverged from intent and cascade from there. No short-circuit.
- **If unsure**: use the full workflow.

Stop after each phase for user review. **Docs carry current intent, written to be read cold** — write each doc as if authored fresh today, from current intent alone: no narration of how it changed, no meaning that needs the conversation that produced it, no rebuttals to questions only a past discussion raised. Rationale, considered alternatives, and constraints a fresh author would independently write stay; record rejected alternatives and why in the LLD's Decisions & Alternatives table, not as asides in body prose.

**Memory vs. intent.** Before saving durable project knowledge to agent or tool memory, test whether it is project *intent* — would a fresh agent, in any tool, next session, need it to build this system correctly? If yes, record it in the arrow (HLD / LLD / EARS / decision doc), which travels and cascades — not in private, per-tool memory, where intent escapes the arrow. Knowledge about the user or how they like to work stays in memory.

### Navigation

| What you need | Where to look |
|---|---|
| High-level design | `docs/high-level-design.md` |
| Design tree (sub-HLDs, LLDs, their specs) | `docs/intent/` — one folder per node |
| EARS specs | beside each design doc as `{node}-specs.md` in the node's folder under `docs/intent/` |
| Decision docs | `docs/decisions/` (project-level) and `docs/intent/<segment>/decisions/` |

### Terminology

- **HLD**: High-Level Design — single project-level doc at `docs/high-level-design.md`.
- **LLD**: Low-Level Design — detailed component design doc in `docs/intent/`. The design layer is a recursive tree: the root is the HLD, leaf LLDs own EARS, and a component deep enough to outgrow one doc becomes a sub-HLD (HLD-shaped, owns no EARS) with children beneath it. "HLD" and "LLD" are roles by position; depth-2 (one HLD over flat leaf LLDs) is the default.
- **EARS**: Easy Approach to Requirements Syntax — structured one-line requirements beside each design doc as `{node}-specs.md` in the node's folder under `docs/intent/`. IDs are path-concatenated — the root-to-leaf path of the owning segment plus a number — so a prefix grep gathers a subtree. Markers: `[x]` implemented, `[ ]` active gap, `[D]` deferred.
- **Arrow**: the unidirectional chain from vision to code (HLD → LLDs → EARS → Tests → Code). Strictly a DAG of intent.
- **Arrow segment**: the territory owned by one leaf LLD — the LLD itself plus the specs, tests, and code that cite its EARS IDs. The boundary is the leaf prefix. Within-segment cascade is free; across-segment cascade pauses.
- **Cascade**: propagating a change downstream through the arrow so adjacent levels stay coherent.

### Code annotations

Annotate code and tests with `@spec` comments citing EARS IDs (Python: `# @spec GARMIN-CAPTURE-001`).

Place the annotation at the *entry point of the behavior's implementation graph* — the topmost function or module owning the specified behavior, not every helper. Tests follow the same rule: annotate the test that directly exercises the spec, not every inner assertion.
