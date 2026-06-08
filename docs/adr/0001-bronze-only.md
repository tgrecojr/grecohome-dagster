# ADR 0001: Bronze-only, daily UTC partitions

## Status
Accepted.

## Context
The predecessor app (`whoopster`) wrote "silver" rows to Postgres and used a 15-minute
APScheduler poll plus a windowed reconciliation that deleted DB rows the API no longer
returned. Postgres is being phased out entirely; the app should do only API → raw capture.

## Decision
- **Bronze-only.** Subjects call the source API and write raw responses to bronze. No Postgres,
  no SQLAlchemy, no Alembic, no APScheduler. Silver/gold are a future phase (built downstream
  off bronze, e.g. with `AutomationCondition`).
- **Daily partitions, UTC fetch-slices.** Bronze is partitioned daily by record date in UTC. A
  partition is a *fetch window* `[day 00:00, next day 00:00)`, **not** a semantic local day. We
  use `end_offset=1` so the in-progress current day is a valid, materializable partition.
- **Hourly schedule over a trailing window.** One hourly schedule re-materializes the trailing
  `reconcile_window_days + 1` (=8) partitions. Whoop retroactively rescores/deletes records, so
  re-capturing the recent window — combined with content-hash dedup — is what keeps bronze
  correct. The 15-min → hourly relaxation is intentional; cadence affects freshness, not
  correctness.
- **Reconciliation moves downstream.** The old delete-from-Postgres reconciliation is gone;
  bronze just appends + dedups. Deletions/rescores are resolved at read/silver time.

## Consequences
- No DB to operate, migrate, or pool. The single hard dependency is a writable `BRONZE_ROOT`.
- Local-day ("day"/"night") semantics must be applied at read time over bronze's raw UTC
  timestamps, never assumed from the partition key.
- Backfill is `dagster backfill` over the same assets — no separate backfill script.

## Related
[[0002-dagster-pins]], [[0003-token-file]].
