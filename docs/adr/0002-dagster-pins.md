# ADR 0002: Pin Dagster to the host; one code-location image per subject

## Status
Accepted.

## Context
Dagster runs self-hosted: the daemon + webserver already run on the host. A code location is a
separate gRPC server the host connects to. The daemon/webserver ↔ code-location protocol is
version-coupled.

## Decision
- **Pin `dagster==1.13.8` and all `dagster-*==0.29.8`** to match the host exactly, so the gRPC
  protocol contract stays in sync. A patch mismatch fails code-location loading with opaque
  gRPC errors. Renovate is configured to **not** bump these; they change deliberately, in
  lockstep with the host.
- **One gRPC code-location image per data subject** (`ghcr.io/tgrecojr/grecohome-dagster-<subject>`).
  Each image runs `dagster code-server start -m <subject>.dagster.definitions` and is registered
  in the host `workspace.yaml`. We ship code locations only — never a Dagster instance,
  webserver, or `dagster.yaml`.
- **Host owns concurrency.** The shared `whoop_api` pool's limit is set on the host instance
  via `dagster instance concurrency set whoop_api 1` (stored in the instance Postgres), not in
  the image. In 1.13.8 a per-pool limit can't be named in `dagster.yaml` (its `concurrency.pools`
  only takes `default_limit`/`granularity`); the asset just tags the pool.

## Consequences
- Subjects deploy, fail, and scale independently; one subject's bad deploy can't take down others.
- The build is a CI matrix over subjects; adding a subject = add it to the matrix + workspace.yaml.
- Upgrading Dagster is a coordinated, manual step across the host and every code-location image.
- Other library deps still pin `==`; pure-data deps (`tzdata`) use `>=`.

## Related
[[0001-bronze-only]], [[0003-token-file]]. See [DEPLOYMENT](../DEPLOYMENT.md).
