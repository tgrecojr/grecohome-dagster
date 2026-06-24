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

`dagster==1.13.10` and all `dagster-*==0.29.10` must match the host daemon/webserver so the
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

In Dagster 1.13.10 a **per-pool limit is set in the instance DB via the CLI**, not in
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
- optional: `BRONZE_MONITOR_DIR` — enables the bronze schema-drift check by giving it a
  writable place for baselines, **outside** `BRONZE_ROOT` (bronze stays immutable raw
  capture). Unset → schema-drift no-ops. See `docs/VALIDATION.md`.

Dagster instance env (read by Dagster, per step 3):
- required: `DAGSTER_HOME`, `DAGSTER_POSTGRES_USER`, `DAGSTER_POSTGRES_PASSWORD`,
  `DAGSTER_POSTGRES_HOST`, `DAGSTER_POSTGRES_DB`.

Mount these into the container:

- `BRONZE_ROOT` — writable; where raw captures are written.
- the directory of `WHOOP_TOKEN_PATH` — writable; the OAuth token file is rewritten atomically
  on every refresh (Whoop rotates the refresh token).
- `DAGSTER_HOME` — the directory containing the shared `dagster.yaml`.
- `BRONZE_MONITOR_DIR` (optional) — writable; holds schema-drift baselines. Mount it as a
  **separate** volume from `BRONZE_ROOT` so checks never write under the immutable bronze tree
  (the check refuses to write a baseline that would land inside `BRONZE_ROOT`).

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
  `GDRIVE_SERVICE_ACCOUNT_PATH` to it. Full walkthrough: [GCP service-account setup](#gcp-service-account-setup).
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

### GCP service-account setup

A **service account** (SA) is a non-human Google identity with its own key — no OAuth consent
screen, no token refresh, no user in the loop. We give it read-only access to *only* the one Drive
folder by sharing that folder with the SA's email, exactly as you would share with a person. The SA
can see nothing else in Drive.

**1. Create / pick a GCP project.** [console.cloud.google.com](https://console.cloud.google.com) →
project picker → *New Project* (e.g. `grecohome-data`). An existing project is fine.

**2. Enable the Drive API.** APIs & Services → *Library* → search "Google Drive API" → **Enable**
(in the target project). This is the only API the SA needs.

**3. Create the service account.** IAM & Admin → *Service Accounts* → **Create service account**.
- Name: `lingo-drive-reader` (id becomes `lingo-drive-reader@<project>.iam.gserviceaccount.com`).
- **Skip** the "Grant this service account access to the project" step — **do not** give it any
  project IAM role. Drive access comes from folder *sharing* (step 6), not project IAM; a project
  role would over-grant. Click **Done**.

**4. Create a key.** Open the SA → *Keys* tab → **Add key → Create new key → JSON** → Create. A
JSON file downloads **once** (Google keeps no copy). This is the secret you'll mount — treat it like
a password.

**5. Store the key as a secret (no commit).** Put the JSON into your secrets manager and have
Ansible drop it at the mounted path (e.g. `/secrets/lingo/sa.json`), `0400`, owned by the container
user. Never commit it; `GDRIVE_SERVICE_ACCOUNT_PATH` points at this mount.

**6. Share the glucose folder with the SA — read-only (the scoping step).** This is what restricts
the SA to just the one folder:
- In Google Drive, open the folder you upload Lingo exports to.
- **Share** → paste the SA email (`lingo-drive-reader@<project>.iam.gserviceaccount.com`) →
  role **Viewer** → **uncheck "Notify people"** → Share.
- That single share is the *entire* grant. The SA can list/read this folder and its files and
  nothing else in your Drive. The code requests only the read-only scope
  (`https://www.googleapis.com/auth/drive.readonly`), so even a Viewer share can't be used to write.

**7. Get the folder id for `GDRIVE_FOLDER_ID`.** Open the folder; the id is the last path segment of
the URL: `https://drive.google.com/drive/folders/`**`<THIS_IS_THE_ID>`**.

**8. Verify before deploy.** With the key path and folder id set, list the folder once:

```bash
docker run --rm \
  -e GDRIVE_SERVICE_ACCOUNT_PATH=/secrets/lingo/sa.json \
  -e GDRIVE_FOLDER_ID=<folder-id> -e BRONZE_ROOT=/data/bronze \
  -v /opt/docker/dagster/lingo/secrets:/secrets/lingo:ro \
  --entrypoint python ghcr.io/tgrecojr/grecohome-dagster-lingo:latest \
  -c "from grecohome_lingo import drive; s=drive.get_drive_service(); \
print([f['name'] for f in drive.list_csv_files(s)])"
```

It should print your uploaded CSV names. An empty list with no error means the SA authenticated but
the folder isn't shared with it (or wrong id) — re-check step 6/7. A `403`/`PERMISSION_DENIED` means
the Drive API isn't enabled on the SA's project (step 2).

> **Scope hygiene.** Two independent limits keep this tight: the *share* bounds the SA to one folder
> (data scoping), and the *OAuth scope* in code is `drive.readonly` (capability scoping). Rotate the
> key by creating a new one (step 4), swapping the secret, then deleting the old key from the *Keys*
> tab. Revoke all access instantly by un-sharing the folder.

## Soil / NOAA USCRN (fourth subject)

Soil is **schedule-driven** like Whoop/Garmin, but the source is a **public NOAA file** (the
`hourly02` USCRN product) — **no auth, no secrets, no token/key mount**. It's the simplest subject
to deploy.

- **Image:** `ghcr.io/tgrecojr/grecohome-dagster-soil`, serving `grecohome_soil.dagster.definitions`.
- **Daily UTC partitions + row-slice + dedup.** The source is one ever-growing year file per station
  (`CRNH0203-{year}-{station}.txt`, one row/hour). Each daily partition's `uscrn_bronze_hourly` asset
  fetches the year file and stores **only that UTC date's rows** (`uscrn/hourly`, `dedupe=True`), so a
  few-times-a-day re-capture never re-stores the whole year. A finished day stores once; today
  re-writes only when a new row appears.
- **Schedule:** `uscrn_schedule` (every 6h, UTC) re-materializes the trailing `USCRN_LOOKBACK_DAYS`
  partitions. Older history is reachable via `dagster backfill` over the same asset.

### Host container (`dagster_soil`)

Mirror the others, but **only two mounts** (no credential mount): on `monitoring`, `DAGSTER_HOME` +
`DAGSTER_POSTGRES_*` (run worker → instance Postgres), the `USCRN_*` env (`USCRN_STATION`, optional
`USCRN_BASE_URL` / `USCRN_LOOKBACK_DAYS` / `USCRN_START_DATE`), and the two mounts — the shared
`dagster.yaml` (`DAGSTER_HOME`) and the bronze root (`/opt/datalake/bronze` → `/data/bronze`; the app
writes the `uscrn/` source segment itself).

```yaml
# workspace.yaml
  - grpc_server: { host: dagster_soil, port: 4000, location_name: soil_ingest }
```

```bash
# optional concurrency pool (instance DB); low volume, not critical
dagster instance concurrency set uscrn_api 1
```

Enable `uscrn_schedule` in the UI (schedules, like sensors, are off by default). Its first ticks
capture the recent partitions; backfill the station's history with
`dagster backfill --partition-range 2010-01-01...<today> --job uscrn_capture_job`.

## Silver (cross-subject layer)

Silver is **not** a data subject — it captures nothing. It's a derived, rebuildable layer
that reads immutable bronze and writes typed, deduplicated **Parquet** for analysis. It
makes **no source-API calls** (no auth, no secrets, no token/key mount) and stays **off**
the `*_api` concurrency pools. Its first table is sleep (`silver_sleep`, a FULL OUTER JOIN
of the Garmin and Whoop sleep streams; see [SILVER.md](SILVER.md)).

- **Image:** `ghcr.io/tgrecojr/grecohome-dagster-silver`, serving
  `grecohome_silver.dagster.definitions`. Depends only on `grecohome-core` + DuckDB — the
  bronze API clients are deliberately absent.
- **Cross-code-location lineage:** the silver sleep assets declare their bronze upstreams
  (`garmin_bronze_sleep`, `whoop_bronze_sleep`) by `AssetKey`, so lineage renders across
  code locations. The reads themselves are **filesystem reads of `BRONZE_ROOT`**, not gRPC
  calls into the subject locations.
- **Whole-table rebuild.** Assets are unpartitioned; each run overwrites its Parquet from
  current bronze (idempotent — last run wins). `silver_sleep_daily` (06:00 UTC, after the
  day's bronze sleep lands) rebuilds the three sleep assets; `silver_checks_daily` (07:00
  UTC) runs the silver asset checks independently. Enable both in the UI (schedules are off
  by default).

### Host container (`dagster_silver`)

Mirror the others — on `monitoring`, `DAGSTER_HOME` + `DAGSTER_POSTGRES_*` (run worker →
instance Postgres) — but the data mounts differ from a subject's: silver **reads** bronze
and **writes** a separate silver root.

- required app env: `BRONZE_ROOT`, `SILVER_ROOT`.
- optional app env: `SILVER_MONITOR_DIR` — reserved for the forthcoming silver
  monitor/validation (mirrors `BRONZE_MONITOR_DIR`); unused today, declared + mounted now so
  turning it on needs no deploy change. Unset → future silver checks no-op.

Mounts (note bronze is **read-only** here — silver never writes under it):

- **`BRONZE_ROOT` — read-only** (`/opt/datalake/bronze` → `/data/bronze:ro`). Silver only
  reads the `garmin/` and `whoop/` source segments.
- **`SILVER_ROOT` — writable**, a **separate** volume outside bronze (e.g.
  `/opt/datalake/silver` → `/data/silver`). The atomic Parquet writer refuses any path under
  `BRONZE_ROOT`.
- **`SILVER_MONITOR_DIR`** (optional) — writable, **separate** from `SILVER_ROOT` (e.g.
  `/opt/docker/dagster/silver/monitor` → `/monitor/silver`), so future checks never write
  under the silver Parquet tree.
- `DAGSTER_HOME` — the directory containing the shared `dagster.yaml`.

```yaml
# workspace.yaml
  - grpc_server: { host: dagster_silver, port: 4000, location_name: silver }
```

No concurrency pool is needed — silver makes no source calls. To rebuild on demand (e.g.
after a bronze backfill), materialize the job rather than backfilling partitions:

```bash
dagster job execute --job silver_sleep_job   # whole-table rebuild from current bronze
```

## Gold (cross-layer marts)

Gold is the analysis layer — derived, rebuildable marts built **from silver** (not bronze).
It reads `SILVER_ROOT` and writes marts under a new `GOLD_ROOT`; like silver it makes **no
source-API calls** (no auth/secrets) and stays **off** the `*_api` pools. Its first mart is
`gold_daily_wellness` (one row per local day joining sleep + recovery + workout load + glucose
summary; see [GOLD.md](GOLD.md)).

- **Image:** `ghcr.io/tgrecojr/grecohome-dagster-gold`, serving `grecohome_gold.dagster.definitions`.
  Depends only on `grecohome-core` + DuckDB.
- **Cross-code-location lineage:** the mart declares its four silver upstreams (`silver_sleep`,
  `silver_recovery`, `silver_workouts`, `silver_glucose`) by `AssetKey`; the reads are
  **filesystem reads of `SILVER_ROOT`**, not gRPC calls.
- **Runs after silver.** `gold_wellness_daily` (07:30 UTC) rebuilds the mart once silver's daily
  rebuilds (≤ 06:50) and silver checks (07:00) have run; `gold_checks_daily` (08:00 UTC) runs the
  gold checks. Enable both in the UI. On-demand: `dagster job execute --job gold_wellness_job`.

### Host container (`dagster_gold`)

Mirror silver — on `monitoring`, `DAGSTER_HOME` + `DAGSTER_POSTGRES_*` (run worker) — but the data
mounts are silver→gold:

- required app env: `SILVER_ROOT`, `GOLD_ROOT`.
- optional app env: `GOLD_MONITOR_DIR` — reserved for a future gold monitor (mirrors
  `*_MONITOR_DIR`); unused today, declared + mounted now so turning it on needs no deploy change.

Mounts (silver is **read-only** here — gold never writes under it):

- **`SILVER_ROOT` — read-only** (`/opt/datalake/silver` → `/data/silver:ro`).
- **`GOLD_ROOT` — writable**, a **separate** volume outside silver (e.g. `/opt/datalake/gold` →
  `/data/gold`). The atomic writer refuses any path under `SILVER_ROOT`.
- **`GOLD_MONITOR_DIR`** (optional) — writable, separate from `GOLD_ROOT`.
- `DAGSTER_HOME` — the directory containing the shared `dagster.yaml`.

```yaml
# workspace.yaml
  - grpc_server: { host: dagster_gold, port: 4000, location_name: gold }
```

No concurrency pool is needed — gold makes no source calls.

## Grafana (data-lake dashboards)

Grafana reads the lake **directly** — no query service, no Postgres. The Grafana
container mounts the lake roots read-only (`/data/bronze`, `/data/silver`, `/data/gold`)
and a **DuckDB datasource plugin** (`motherduck-duckdb-datasource`, in-memory/local mode)
runs `read_parquet('/data/<layer>/...')` straight off those mounts. Each panel is a SQL
query, so the daily silver/gold rebuilds show up with no restart.

- **Mounts (read-only):** `/data/bronze`, `/data/silver`, `/data/gold` into the Grafana
  container. The read-only, lake-only mount is the security boundary (a query can only
  ever read those paths).
- **Panel target shape:** `format` must be the **integer** enum (`1` = table), not the
  string `"table"` — the plugin's Go backend rejects a string with
  *"cannot unmarshal string into Go struct field Query.format"*.
- **Dashboards:** `Daily Wellness` (`/d/daily-wellness`, on `gold/wellness`) and `Sleep`
  (`/d/sleep-lake`, on `silver/sleep`), both in the *Health (data lake)* folder.

> A standalone `grecohome-lakequery` HTTP service (DuckDB-over-Parquet on :9999) was the
> earlier serving approach; it was retired in favor of the direct DuckDB datasource above.
> Resurrect it from git history if a remote/HTTP query boundary is ever needed.

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
