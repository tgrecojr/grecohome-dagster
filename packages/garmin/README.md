# grecohome-garmin

The Garmin data subject: a **bronze-only** Dagster code location that captures the
full Garmin Connect data surface (allowlisted) to the bronze layer. Ported from
`garmincapture` onto `grecohome-core`.

- Auth fully delegated to the `garminconnect` library; a one-time interactive MFA
  bootstrap writes a token store at `GARMINTOKENS` (mounted, writable, never under
  `BRONZE_ROOT`).
- An allowlisted endpoint **catalog** (Buckets A/B/C) drives every call; mutating/
  auth methods are never callable, and a drift detector surfaces new readable
  endpoints. Two capture grades: reserialized JSON, and raw FIT `.zip` downloads.
- One asset per collection (daily-partitioned for date-oriented data, unpartitioned
  for reference/snapshots), captured **append-only, no dedup** (Garmin data is
  immutable). Backfill via `dagster backfill`.

Ships as a per-subject gRPC **code-location image** (`grecohome-dagster-garmin`)
that registers with the host Dagster daemon/webserver. See `docs/DEPLOYMENT.md`.
