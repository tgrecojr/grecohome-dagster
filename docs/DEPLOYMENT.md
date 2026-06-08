# Deployment

Each data subject ships as its own **gRPC code-location image** that registers with
the **existing host Dagster daemon + webserver**. We deploy code locations only — never
a Dagster instance, webserver, or `dagster.yaml` (those live on the host).

## Images

Per-subject images are published to GHCR on every push to `main`:

```
ghcr.io/tgrecojr/grecohome-dagster-whoop:latest
ghcr.io/tgrecojr/grecohome-dagster-whoop:whoop-<sha>
```

Each image runs `dagster code-server start` serving its subject's `Definitions` on port
4000. Images are multi-arch (amd64/arm64), cosign-signed, and ship an SBOM + SLSA
provenance attestation.

## Pins (hard requirement)

`dagster==1.13.8` and all `dagster-*==0.29.8` must match the host daemon/webserver so the
daemon ↔ code-location gRPC protocol stays in sync. A mismatch fails code-location loading
with opaque gRPC errors. Renovate is configured **not** to bump these; update them
deliberately, in lockstep with the host.

## Host wiring

### 1. Register the code location (`workspace.yaml`)

On the host, point Dagster at each subject's gRPC server (Ansible-managed):

```yaml
load_from:
  - grpc_server:
      host: whoop-code-location   # docker network service / hostname
      port: 4000
      location_name: whoop
  # Future subjects:
  # - grpc_server: { host: garmin-code-location, port: 4000, location_name: garmin }
  # - grpc_server: { host: lingo-code-location,  port: 4000, location_name: lingo }
```

### 2. Shared Whoop-API concurrency pool

The Whoop assets tag their ops with the `whoop_api` concurrency pool. **The limit is
enforced by the host instance, not the image.** Set it to 1 so the hourly tick and any
backfill cannot collectively exceed the API budget (and only one run holds the OAuth token
at a time, avoiding the refresh-token rotation race).

Either in the host `dagster.yaml`:

```yaml
concurrency:
  pools:
    whoop_api:
      max_concurrent: 1
```

or via the CLI:

```bash
dagster instance concurrency set whoop_api 1
```

### 3. Required environment / mounts

Inject per-subject env at deploy (Ansible + secrets manager). For Whoop, see
`docs/ENV_TEMPLATE.md` — required: `BRONZE_ROOT`, `WHOOP_CLIENT_ID`, `WHOOP_CLIENT_SECRET`,
`WHOOP_TOKEN_PATH`.

Mount two writable volumes into the container:

- `BRONZE_ROOT` — where raw captures are written.
- the directory of `WHOOP_TOKEN_PATH` — the OAuth token file is rewritten atomically on
  every refresh (Whoop rotates the refresh token), so it must be writable.

## One-time OAuth setup

The token file at `WHOOP_TOKEN_PATH` is created by the interactive OAuth flow:

```bash
# Headless (server): prints an auth URL, you paste back the callback URL.
python -m grecohome_whoop.oauth_setup --headless

# Local with a browser + callback server:
python -m grecohome_whoop.oauth_setup
```

Run it with the same `WHOOP_*` env and a writable `WHOOP_TOKEN_PATH` so the resulting token
lands where the code location will read it.

## Backfill

Backfill older partitions through the same assets (no separate script):

```bash
dagster backfill --partition-range 2024-01-01...2024-03-31 \
  --job whoop_bronze_job
```

The `whoop_api` pool keeps backfill within the API budget alongside the hourly schedule.

## Building locally

```bash
docker build -f packages/whoop/Dockerfile -t grecohome-dagster-whoop:dev .
docker run --rm \
  -e WHOOP_CLIENT_ID=... -e WHOOP_CLIENT_SECRET=... \
  -e BRONZE_ROOT=/data/bronze -e WHOOP_TOKEN_PATH=/secrets/whoop/token.json \
  -v /local/bronze:/data/bronze -v /local/secrets:/secrets/whoop \
  -p 4000:4000 grecohome-dagster-whoop:dev
# health: docker exec <id> dagster api grpc-health-check -p 4000
```
