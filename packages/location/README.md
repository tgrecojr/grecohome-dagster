# grecohome-location

Phone **location** data subject for `grecohome-dagster` — a **bronze-only** Dagster code location.

It **promotes** the raw staging files produced by the external
[`locationrelay`](https://github.com/tgrecojr) Rust service (a hardened, internet-facing HTTP
receiver) into the bronze lake via the shared `capture_bronze` writer. *Relay stages raw; Python
promotes to bronze* — the lake stays single-writer (Python) and the bronze contract stays
single-sourced in `grecohome_core`. The relay also forwards to Dawarich in real time; bronze is the
durable audit/replay copy.

## Streams → collections

Two fixed collections, one per relay staging subdir / ingest route:

- `location/overland` — the exact Overland batch body (`{"locations":[…]}`, envelope intact).
- `location/owntracks` — the exact OwnTracks message body, verbatim.

## How it works

Every few minutes a scheduled asset (one per stream) scans the relay's trailing staging window
(`LOCATION_PROMOTE_WINDOW_DAYS`) and lands each **new** file in bronze byte-for-byte
(`capture_mode="raw"`, `sha256(bronze) == sha256(staging)`). `dt` is the **receipt** date parsed
from the staging path; the true receipt time (`received_at` / `received_unix_ms`, from the filename)
and the `staging_file` basename go in the sidecar. `fetched_at` is the *promote* time.

**Idempotency** is keyed on the staging **filename** (never content, so two byte-identical distinct
POSTs both land): a per-stream promoted-set in `LOCATION_STATE_DIR`, backstopped by the
`staging_file` recorded in each bronze sidecar (which makes promotion exactly-once in bronze across
crashes and rebuilds a lost promoted-set).

## Deployment constraints

- **Run the container as uid 1000 at runtime.** The image builds like every other code location
  (runs as `nonroot`); the deployment sets the runtime user (e.g. compose `user: "1000:998"`).
  Staging files are `0600` owned by uid 1000, so only uid 1000 can read them.
- **Mount `RELAY_CAPTURE_DIR` read-only** (host `/opt/docker/locationrelay/data`). The promoter
  never writes/deletes under it — the relay's retention janitor is the sole cleaner.
- **`LOCATION_STATE_DIR` must be outside `BRONZE_ROOT`** (enforced at startup) and writable.
- Keep `LOCATION_PROMOTE_WINDOW_DAYS` comfortably larger than worst-case promoter downtime and
  smaller than the relay's `LOCATIONRELAY_RETENTION_DAYS` (default 14) so nothing is pruned before
  promotion. The **promote-lag** check (ERROR) is the early guardrail.

See [docs/LOCATION.md](docs/LOCATION.md), [docs/ENV_TEMPLATE.md](../../docs/ENV_TEMPLATE.md),
[docs/BRONZE.md](../../docs/BRONZE.md).
