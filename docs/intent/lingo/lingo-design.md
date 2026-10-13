---
parent: high-level-design
prefix: LINGO
---

# Lingo (CGM glucose) bronze subject

## Context and Design Philosophy

`grecohome_lingo` (code location `lingo_ingest`) captures Abbott Lingo continuous-glucose
readings to bronze. It replaced the standalone `glucose-loader` service.

Lingo has **no API**. The user exports a CSV from the Lingo iOS app, sporadically, and
uploads it to a Google Drive folder. Each export is **cumulative**: every reading to date, not a
delta. So this subject isn't driven by a clock. It's driven by **files arriving**: a sensor
watches the folder and captures each new file once.

Two things follow:

- **"Captured" is per Drive file.** Each file gets one Dagster dynamic partition, keyed by its
  Drive `file_id`. The partition set itself is the ledger of what's been captured, replacing
  `glucose-loader`'s Postgres `ProcessedFile` table, in line with the HLD's no-database rule.
- **Bronze `dt` is the capture date, not an event date.** A cumulative export has no single
  event date, so the file lands in the folder for the day it was captured. The real reading
  times live inside the CSV, and silver and the completeness check read them from there.

## Sensor and partitions

`lingo_drive_sensor` lists the watched folder at most every `GDRIVE_POLL_INTERVAL_MINUTES`
(default 5; read when the code location loads, so a change needs a restart). It runs in the
Lingo code location, where the Drive credential is mounted; the host daemon only triggers it. Its
runs execute `lingo_capture_job`.

- **Listing:** files whose parent is `GDRIVE_FOLDER_ID`, whose Drive MIME type is exactly
  `text/csv`, and which aren't trashed, ordered by creation time, paginating until Drive returns
  no further page token. A file Drive labels differently (`application/vnd.ms-excel`,
  `application/octet-stream`, a converted Sheet) is never seen, with no signal. The listing doesn't
  request shared-drive items, so the folder must be in a regular "My Drive".
- **New files:** every listed `file_id` that isn't already a partition of `lingo_files` is added
  as a partition, and a run is requested for it with `run_key = file_id`. Sensors dedupe run keys
  across all evaluations, so each file is captured at most once. Several new files produce
  several runs from one tick.
- **Nothing new:** the tick is skipped with "no new Drive files".
- **First tick:** every file in the folder is new, so the existing backlog is captured. There is
  no daily partition grid and no backfill path.
- **A Drive error while listing** fails the tick. No partitions are added (they're only added
  through a successful tick's result), and the next tick tries again.

The sensor starts **stopped**, like every capture schedule and sensor (HLD), and is enabled
after the service account is set up (`docs/DEPLOYMENT.md` lists it).

**A file updated in place isn't re-captured.** The sensor only looks at whether a `file_id` is
new. If a Drive file's content were replaced under the same id, its new version would never be
captured. Each Lingo export is a new file, so this doesn't happen in practice; a changed export
means uploading a new file.

## Capture

`lingo_bronze_glucose` is the only asset, partitioned on `lingo_files` and tagged with the
`lingo_api` pool. For its partition's `file_id`, it:

1. reads the file's metadata (`name`, `createdTime`, `modifiedTime`) from Drive;
2. downloads the file's content into memory (no temporary file), in chunks;
3. captures the bytes to `lingo/glucose/dt=<UTC capture date>/` as `text/csv` (`ext=csv`),
   byte-for-byte, with **skip-identical off** (`dedupe=False`);
4. records `file_id`, `file_name`, `bytes` and `bronze_path` as materialization metadata.

**A failed bronze write fails the run.** The core writer never raises: a failed write (a full
disk, permissions) only logs a warning and returns no path. With skip-identical off, no path can
only mean failure, so the asset raises. Otherwise the run would succeed, the partition would look
captured, and the sensor would never request that file again: the export would be silently lost.

The sidecar carries Drive provenance only:
- `request_url`: the file's Drive media URL, which carries no token;
- `request_params`: `file_id`, `file_name`, `drive_folder_id`, `created_time`, `modified_time`.

The service-account key never reaches bronze or the sidecar.

**Why skip-identical is off.** The partition per `file_id` already guarantees each file is
captured once. Bronze folders are by capture date, not by file. With skip-identical on, two
*different* Drive files that happen to be byte-identical and are captured on the same day would
collapse to one payload: the second file's bronze record would be silently dropped while its
partition stayed marked captured. A re-upload of the same export under a new id is therefore kept
as its own capture.

Any Drive error (metadata, download) raises and fails the run. Because the partition and run key
already exist, the sensor won't request that file again. The same holds if the daemon adds a
partition but its run is never created. A stranded file is captured by re-executing its run, or by
materializing its partition from the UI.

The asset fetches whatever `file_id` its partition names, without checking the folder, so a
partition added by hand captures any file the service account can read.

Successive uploads land in successive `dt=` folders, each a separate capture of a cumulative
export. Silver reads every capture and keeps one reading per instant.

## Authentication

A Google **service account**, not OAuth: there's no consent screen, no token and nothing to
refresh or rotate automatically.

- The mounted key JSON at `GDRIVE_SERVICE_ACCOUNT_PATH` (never under `BRONZE_ROOT`) builds a
  Drive v3 client with **only** the `drive.readonly` scope.
- Access is granted by **sharing the one watched folder (Viewer) with the service account's
  email**. That share is the entire grant: the service account has no project IAM role.
- **Two independent limits:** the share bounds it to one folder, and the read-only scope bounds
  it to reading.
- **Key rotation:** create a new key, swap the mounted secret, delete the old key.
- **Instant revocation:** un-share the folder.

A bad or missing key file fails every sensor tick loudly. But a **wrong, empty or un-shared
folder** doesn't: Drive just returns an empty listing, so the sensor skips quietly, exactly as it
does when there's nothing new. Revoking access by un-sharing looks the same. The folder-health
check (below) is what catches it. Setup steps are in `docs/DEPLOYMENT.md` (*GCP service-account
setup*).

## Bronze checks

One `CollectionCheckConfig` for `lingo/glucose`, run hourly by `lingo_bronze_checks_job`
(`lingo_bronze_checks_hourly`, which starts running; checks use no pool):

| Setting | Value | Why |
|---|---|---|
| `reader` | `csv` | The export is CSV |
| `event_date_source` | `payload`, column `Time of Glucose Reading [T=(local time) +/- (time zone offset)]` | `dt` is the capture date; reading times are in the file (values like `2026-06-05T21:45-04:00`). Overlapping rows across cumulative exports collapse to distinct dates |
| Completeness | gap over **14 days** (WARN) | The sensor is worn in bursts, so long gaps are normal |
| Freshness | **off** | Uploads are sporadic; "no recent upload" is usually non-wear, not a break, and an ERROR would cry wolf |
| Schema drift, content health | on | |

**`lingo_drive_folder_health`** is a bespoke asset check (ERROR, alerting through
`asset_check_failed`) on `lingo_bronze_glucose`, run in the same hourly checks job:
- It lists the watched folder with the same query the sensor uses.
- It **fails** when listing raises (bad key, API disabled, network), or when it returns **zero
  CSV files**. The folder only accumulates exports, so an empty listing means a wrong folder id,
  a missing share or revoked access, never a quiet week.
- It reports only the folder id, the number of files found, or the error's type name. It never
  reports credentials or file contents.

Unlike the other bronze checks, it calls the Drive API, which is a cheap read-only list.

## Configuration

| Variable | Default | Meaning |
|---|---|---|
| `GDRIVE_FOLDER_ID` | **required** (no default) | The watched folder's stable Drive id |
| `GDRIVE_SERVICE_ACCOUNT_PATH` | **required** (no default) | The mounted service-account key JSON |
| `GDRIVE_POLL_INTERVAL_MINUTES` | 5 | Minimum interval between sensor ticks |
| `BRONZE_ROOT`, `BRONZE_MONITOR_DIR` | (required; optional) | Shared bronze root and check-state directory |

## Decisions & Alternatives

| Decision | Chosen | Alternatives Considered | Rationale |
|---|---|---|---|
| Trigger | A Drive-folder sensor | A schedule polling the folder | Data arrives when the user uploads, not on a clock. A sensor dedupes run keys across evaluations, which makes "capture each file once" native. |
| Capture ledger | The dynamic partition set (one partition per `file_id`) | A Postgres `ProcessedFile` table | No transactional database (HLD), and the partitions are visible in Dagster. |
| Skip-identical | Off | On, as a backstop against re-uploads | Bronze folders are by capture date, so content dedup could drop a distinct, byte-identical file captured the same day while its partition looked captured. The per-file partition is the capture-once guard. |
| Bronze `dt` | Capture date | An event date | A cumulative export spans many days; event dates are read from inside the CSV. |
| Auth | Service account with a folder share and the read-only scope | OAuth with a refresh token | No consent flow and no rotating token to lose. The share plus the scope keep the grant to "read one folder". |
| Freshness | Off; completeness at 14 days | Freshness with a long tolerance | Sporadic uploads would make any freshness tolerance either useless or noisy. |
| Folder health | `lingo_drive_folder_health`: ERROR when listing fails or finds no CSVs | Rely on sensor tick failures; an ERROR log line on an empty listing | A wrong or un-shared folder produces an empty listing, not an error, so the sensor skips silently and nothing alerts (freshness is off). The folder only accumulates exports, so empty is always a fault. |
| Failed bronze write | The asset raises | Trust the writer's non-fatal `None` | A swallowed failure leaves the partition marked captured; the sensor never retries, so the export is lost for good. |
| Required settings | `GDRIVE_FOLDER_ID` and `GDRIVE_SERVICE_ACCOUNT_PATH` have no default | Empty-string defaults | An empty folder id runs silently misconfigured. Missing settings should stop the code location at startup with the standard configuration error. |

## Open Questions & Future Decisions

### Deferred

1. **The `grpcio` floor.** Google's client libraries (`google-api-core`, used only here) are
   raising their minimum to `grpcio>=1.83`. The workspace pins `grpcio==1.81.1` until Dagster
   moves to protobuf 7. When the upgrade can't resolve, the options in order are:
   1. adopt a Dagster release on protobuf 7 and move grpc in lockstep with the host;
   2. pin the Google client libraries below the new floor, for this package only;
   3. let `grpcio` (never `grpcio-health-checking`) float to ≥1.83 if it resolves against the host
      contract.
2. **Files updated in place.** Comparing Drive's `modifiedTime` against the captured partition
   would catch a replaced file. It isn't needed while every export is a new file.

## References

- `packages/lingo/src/grecohome_lingo/`: `drive.py`, `capture.py`, `dagster/`
- `docs/DEPLOYMENT.md`: Lingo container (image `ghcr.io/tgrecojr/grecohome-dagster-lingo`), the
  optional `lingo_api` pool limit, GCP service-account setup
