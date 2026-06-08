# grecohome-whoop

The Whoop data subject: a **bronze-only** Dagster code location that captures raw
Whoop API responses to the bronze layer. No Postgres, no silver — just
API → bronze.

- Daily UTC-partitioned bronze assets (`sleep`, `recovery`, `workout`, `cycle`) plus an
  unpartitioned `snapshots` asset (`profile`, `body_measurement`).
- One hourly schedule re-capturing the trailing 8 daily partitions (7-day reconcile
  overlap + 1 settle), with content-hash dedup at capture.
- Backfill via `dagster backfill` over the same assets.
- OAuth refresh token persisted as a plaintext JSON file at `WHOOP_TOKEN_PATH`.

Ships as a per-subject gRPC **code-location image** that registers with the host
Dagster daemon/webserver. See `docs/DEPLOYMENT.md`.
