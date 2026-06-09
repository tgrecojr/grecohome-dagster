# Deployment

Each data subject ships as its own **gRPC code-location image** that registers with
the **existing host Dagster daemon + webserver**. We don't *own* the Dagster instance,
webserver, or `dagster.yaml` (the host does) — but because the host uses the
`DefaultRunLauncher`, each run executes **inside the code-location container**, so that
container does need the host's instance config + Postgres at run time (see
[Dagster instance wiring](#dagster-instance-wiring)).

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
enforced by the host instance (stored in its Postgres), not the image.** Set it to 1 so the
hourly tick and any backfill cannot collectively exceed the API budget (and only one run
holds the OAuth token at a time, avoiding the refresh-token rotation race).

In Dagster 1.13.8 a **per-pool limit is set in the instance DB via the CLI**, not in
`dagster.yaml`. (The `concurrency.pools` block in `dagster.yaml` only accepts
`default_limit` / `granularity` / `op_granularity_run_buffer` — naming a pool there, e.g.
`pools: { whoop_api: { max_concurrent: 1 } }`, is invalid and crashes the daemon at instance
load.) Set it once, against the running instance (any container with `DAGSTER_HOME` + the
`DAGSTER_POSTGRES_*` env — the daemon works):

```bash
dagster instance concurrency set whoop_api 1
dagster instance concurrency get whoop_api   # verify
```

This is an **op**-granularity limit, so at most one `whoop_api` op runs across all runs. It
persists in the instance DB (re-run it if you ever reset that DB). To keep it Ansible-managed,
run it as a post-deploy `docker_container_exec` against the daemon container:

```yaml
- name: Set whoop_api pool concurrency limit
  community.docker.docker_container_exec:
    container: dagster_daemon
    command: dagster instance concurrency set whoop_api 1
  changed_when: false
```

> A blanket `concurrency: { pools: { default_limit: 1 } }` in `dagster.yaml` is valid and
> would also cap `whoop_api` while it's the only pool — but it limits *every* pool, so prefer
> the per-pool CLI for precision.

### 3. Dagster instance wiring

Because the host uses the `DefaultRunLauncher` (no `run_launcher` block), each run executes
as a subprocess **inside the code-location container**, and that subprocess writes to the
host instance's Postgres event/run/schedule storage. So the container must share the host's
instance config:

- **Mount the daemon's `dagster.yaml`** into the container and set **`DAGSTER_HOME`** to its
  directory. It's the same file the daemon uses (carries no secrets — it references DB creds
  by env-var name). Don't bake it into the image.
- **Provide the Postgres env vars** the `dagster.yaml` references:
  `DAGSTER_POSTGRES_USER`, `DAGSTER_POSTGRES_PASSWORD`, `DAGSTER_POSTGRES_HOST`,
  `DAGSTER_POSTGRES_DB` (port is set in `dagster.yaml`). The container must be able to reach
  that Postgres.
- `dagster-postgres` is bundled in the image, so the run subprocess can instantiate the
  Postgres storage.

The host `dagster.yaml` also carries the `whoop_api` pool from step 2; mounting the same file
into the container is harmless (the limit is enforced by the daemon's run coordinator).

### 4. Required environment / mounts

Inject per-subject env at deploy (Ansible + secrets manager). See `docs/ENV_TEMPLATE.md`.

App env (read by the app's settings):
- required: `BRONZE_ROOT`, `WHOOP_CLIENT_ID`, `WHOOP_CLIENT_SECRET`, `WHOOP_TOKEN_PATH`.

Dagster instance env (read by Dagster, per step 3):
- required: `DAGSTER_HOME`, `DAGSTER_POSTGRES_USER`, `DAGSTER_POSTGRES_PASSWORD`,
  `DAGSTER_POSTGRES_HOST`, `DAGSTER_POSTGRES_DB`.

Mount three things into the container:

- `BRONZE_ROOT` — writable; where raw captures are written.
- the directory of `WHOOP_TOKEN_PATH` — writable; the OAuth token file is rewritten atomically
  on every refresh (Whoop rotates the refresh token).
- `DAGSTER_HOME` — the directory containing the shared `dagster.yaml`.

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

## Garmin (second subject)

Garmin deploys the same way as Whoop (per-subject gRPC code-location image registered with the
host daemon over `workspace.yaml`, runs executing in the container via `DefaultRunLauncher`), with
a few subject-specific differences:

- **Image:** `ghcr.io/tgrecojr/grecohome-dagster-garmin`, serving
  `grecohome_garmin.dagster.definitions`.
- **Auth is delegated to `garminconnect`** — there is no token *file*; instead a token *store*
  directory at **`GARMINTOKENS`** (mounted, writable, **separate from bronze**). The library
  self-heals/refreshes it. Required env: `GARMINCONNECT_EMAIL`, `GARMINCONNECT_BASE64_PASSWORD`
  (+ optional `GARMINCONNECT_IS_CN`); tuning: `LOOKBACK_DAYS`, `FETCH_SELECTION`/`FETCH_EXCLUDE`
  (default empty = capture all), `RATE_LIMIT_SECONDS`, `WEEKLY_WEEKS`, `CAPTURE_ALT_FORMATS`.
- **No dedup** (Garmin data is immutable) — the daily schedule captures each completed partition
  **exactly once** (`run_key = partition_key`), trailing `LOOKBACK_DAYS`. A second daily schedule
  refreshes the unpartitioned reference/snapshot collections.

### Host container (`dagster_garmin`)

Mirror the `dagster_whoop` task: on the `monitoring` network, `DAGSTER_HOME` + the four
`DAGSTER_POSTGRES_*` (run worker → instance Postgres), the `GARMINCONNECT_*` env, and **three
mounts** — the shared `dagster.yaml` (`DAGSTER_HOME`), the bronze root
(`/opt/datalake/bronze` → `/data/bronze`; the app writes the `garmin/` source segment itself), and
a writable **`GARMINTOKENS`** dir (e.g. `/opt/docker/dagster/garmin/tokens` → `/secrets/garmin`,
matching Whoop's `/secrets/<subject>` convention).

```yaml
# workspace.yaml
  - grpc_server: { host: dagster_garmin, port: 4000, location_name: garmin_ingest }
```

```bash
# concurrency pool (instance DB), same as whoop
dagster instance concurrency set garmin_api 1
```

### One-time MFA bootstrap

Garmin's first login needs an interactive MFA code; run it once to write the token store:

```bash
docker run --rm -it \
  -e GARMINCONNECT_EMAIL=... -e GARMINCONNECT_BASE64_PASSWORD=... \
  -e GARMINTOKENS=/secrets/garmin -e BRONZE_ROOT=/data/bronze \
  -v /opt/docker/dagster/garmin/tokens:/secrets/garmin \
  --entrypoint python ghcr.io/tgrecojr/grecohome-dagster-garmin:latest \
  -m grecohome_garmin.bootstrap
```

### Backfill

```bash
dagster backfill --partition-range 2024-01-01...2024-03-31 --job garmin_daily_job
```

Because Garmin has no dedup, backfilling an already-captured day **appends** a second copy
(intended for explicit re-pulls; clean for never-captured history). The `garmin_api` pool keeps a
large backfill gentle.

## Lingo (third subject)

Lingo is **file-arrival-driven**, not schedule-driven: the user exports CGM data from the Lingo
iOS app and uploads a cumulative CSV to a Google Drive folder; a **sensor** captures each new file.

- **Image:** `ghcr.io/tgrecojr/grecohome-dagster-lingo`, serving `grecohome_lingo.dagster.definitions`.
- **Auth = Google service account** (no interactive OAuth). In GCP, create a service account, enable
  the Drive API, download its key JSON; in Drive, **share the watched folder (read-only) with the
  SA's email**. Mount the key JSON (e.g. `/secrets/lingo/sa.json`) and set
  `GDRIVE_SERVICE_ACCOUNT_PATH` to it. (Full walkthrough below / from the agent.)
- **Sensor + dynamic partitions:** `lingo_drive_sensor` lists the folder and adds a partition +
  run per new Drive `file_id` (one collection: `lingo/glucose`, captured once each). **Enable the
  sensor** in the UI/daemon — sensors are off by default. No schedule, no backfill grid; the
  sensor's first tick captures the existing folder backlog.

### Host container (`dagster_lingo`)

Mirror the others: on `monitoring`, `DAGSTER_HOME` + `DAGSTER_POSTGRES_*` (run worker), the
`GDRIVE_*` env (`GDRIVE_FOLDER_ID`, `GDRIVE_SERVICE_ACCOUNT_PATH`, optional
`GDRIVE_POLL_INTERVAL_MINUTES`), and **three mounts** — the shared `dagster.yaml`, the SA key
(read-only, e.g. `/secrets/lingo`), and the bronze root (`/opt/datalake/bronze` → `/data/bronze`;
the app writes the `lingo/` source segment itself).

```yaml
# workspace.yaml
  - grpc_server: { host: dagster_lingo, port: 4000, location_name: lingo_ingest }
```

The sensor runs in this container (where the SA key lives); the host daemon triggers its
evaluation over gRPC. Optional `dagster instance concurrency set lingo_api 1` (low volume; not
critical).

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
