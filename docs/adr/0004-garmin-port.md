# ADR 0004: Garmin port — per-collection assets, no dedup, delegated auth

## Status
Accepted.

## Context
Garmin is the second data subject, ported from the standalone `garmincapture` service into the
monorepo. Unlike Whoop (a clean OAuth API the app calls directly), Garmin Connect has no
official personal API: access is via the `garminconnect` library, the data surface is large
(~65 collections across daily/range/weekly/static/per-activity/per-device shapes), and the data
is **immutable** (Garmin does not rescore history).

## Decision
- **Reuse `grecohome-core`** for capture, settings, logging, and Dagster helpers. Garmin-specific
  logic (`catalog`, `serialize`, `auth`, `pull`) lives in `packages/garmin`. `garmincapture`'s own
  `bronze.py` is dropped in favor of core's writer.
- **Auth delegated to `garminconnect`.** No token *file* / OAuth client of our own; the library
  owns a token *store* at `GARMINTOKENS` (mounted, writable, never under `BRONZE_ROOT`), with a
  one-time interactive MFA bootstrap. Core's `TokenFileStore` is **not** used.
- **Allowlisted catalog** is the call recipe; mutating/auth methods are never invoked, a unit test
  enforces it, and a drift detector surfaces new readable endpoints for review. Two capture grades
  (reserialized JSON, raw FIT) recorded via `capture_mode`; profile/settings are secret-screened.
- **One asset per collection** (factory over the catalog, ~61). `activities` fans out per-activity
  detail + FIT internally; per-device/per-profile loop discovered ids. Matches Whoop's
  per-collection observability.
- **No content-hash dedup** (`dedupe=False`) — core's dedup is made opt-in. Because data is
  immutable, the daily schedule captures each *completed* partition **exactly once**
  (`run_key = partition_key`, `end_offset=0`) over the trailing `LOOKBACK_DAYS`. Dagster's run-key
  dedup replaces content-hash dedup; re-pulls only happen via explicit `dagster backfill` (which
  appends).
- **`garminconnect` is sync + login is costly**, so a per-run client resource + the in-process
  executor log in **once per run** (not per asset). `garminconnect` pinned `>=0.3.5` (a data dep
  that must follow Garmin API drift, like `tzdata`).
- **`FETCH_EXCLUDE` defaults empty** (capture the full surface); both `FETCH_SELECTION` and
  `FETCH_EXCLUDE` remain runtime knobs (a deselected asset is a no-op).

## Consequences
- The bronze layout gains `garmin/*` collections (incl. always-empty faithful records like `hrv`).
- Per-collection failures are visible/retryable (the pull helpers propagate real errors rather than
  swallowing them as the standalone runner did).
- A new device producing a previously-empty stream begins populating automatically.

## Related
[[0001-bronze-only]], [[0002-dagster-pins]], [[0003-token-file]].
