# Bronze layer

The bronze layer is the raw, immutable record of exactly what each source API returned. It is
the only **source of truth**; the silver and gold layers are derived from it and fully
rebuildable (see [SILVER](SILVER.md), [GOLD](GOLD.md)). Bronze itself is never modified by them.

## Invariants

- **Bytes only.** Payloads are written byte-for-byte from `response.content`, never a decoded
  string. Decoding is a downstream concern.
- **Append-only / immutable.** Every written capture is a new, uniquely named file. Nothing is
  overwritten or deleted by the capture path.
- **Content-hash dedup (opt-in, `dedupe`).** With `dedupe=True` (default) the payload's sha256 is
  compared to the newest capture for the same `(source, collection, dt)` partition and an
  identical payload is skipped — so a source that re-captures an overlapping window (Whoop, which
  rescores) costs API calls but ~zero storage. Immutable sources (Garmin) pass `dedupe=False` and
  rely on **capture-once scheduling** (`run_key = partition_key`) to avoid re-pulls instead.
- **Non-fatal.** `capture_bronze` never raises; failures are logged and swallowed.
- **Swappable root.** The bronze root is passed in by the caller (`BRONZE_ROOT`); nothing is
  hardcoded, keeping an object-store migration open.

## Layout

```
{BRONZE_ROOT}/{source}/{collection}/dt={YYYY-MM-DD}/
    {collection}_{fetched_unix_ms}_{short_id}.{ext}
    {collection}_{fetched_unix_ms}_{short_id}.meta.json
```

- `dt` is the **partition date** the payload belongs to (the asset passes its partition key),
  not necessarily the fetch date — so hourly re-captures of a trailing day dedup against the
  right folder.
- `{ext}` reflects the *stored* form (`json` for reserialized payloads, `zip` for Garmin FIT
  downloads, `bin` for unknown content types). Sources may pass an explicit `ext`.

Example:

```
bronze/whoop/recovery/dt=2026-06-08/recovery_1717900000000_a1b2c3.json
bronze/whoop/recovery/dt=2026-06-08/recovery_1717900000000_a1b2c3.meta.json
```

## Sidecar (`.meta.json`)

Provenance for each payload. Filled by the capture function: `source`, `collection`,
`fetched_at`, `fetched_at_unix_ms`, `byte_size`, `sha256`, `stored_encoding`, `schema_version`
(`v1`). Passed through by the caller: `request_url`, `request_params`, `http_status`,
`content_type`, `charset`, `content_encoding`, `processor`, `processor_version`, and (for Garmin)
`capture_mode` + `redacted_fields`. **Never contains secrets** (no auth headers/tokens).

## Whoop specifics

- Captured inside `_make_request`: successful payloads (skipping empty pagination terminators)
  **and** error bodies (with their real HTTP status, before `raise_for_status`) as diagnostics.
- Collections: `sleep`, `recovery`, `workout`, `cycle` (daily-partitioned), plus `profile` and
  `body_measurement` (current-only snapshots, fetch-date folder).
- Timestamps are stored as-received (UTC); no timezone conversion happens in bronze.

## Garmin specifics

- **Two capture grades** (recorded in `capture_mode`): `reserialized` — the `garminconnect`
  library returns parsed objects, re-serialized via deterministic compact JSON (no `sort_keys`);
  and `raw` — binary downloads (the FIT/original `.zip`) stored byte-for-byte.
- **No dedup** (`dedupe=False`): Garmin data is immutable, so every capture is kept and re-pulls
  are avoided by capture-once scheduling rather than content-hash dedup.
- **Secret-screening:** profile/settings collections (`user_settings`, `userprofile_settings`)
  are run through a recursive secret-key remover before capture; dropped key paths are recorded
  in `redacted_fields`. Tokens/credentials never reach bronze (the token store is a separate
  mount, never under `BRONZE_ROOT`).
- **Allowlist only:** an endpoint catalog is the call recipe; mutating/auth methods are never
  invoked, and a drift detector surfaces new readable endpoints for review.
- **Empty 200s are faithful records** for endpoints without a skip flag (e.g. `hrv`,
  `training_readiness`) — they capture an empty payload and begin populating if a device starts
  producing them.

## Lingo specifics

- **One collection** (`lingo/glucose`); payloads are the **CSV bytes** of each Drive export,
  stored byte-for-byte (`text/csv` → `ext=csv`). No reserialization.
- **`dt` is the fetch/capture date**, not a per-record date: each export is a *cumulative* dump of
  all CGM records to date, so the file belongs to "when it arrived," and successive uploads land in
  successive `dt=` folders.
- **Dedup on** (`dedupe=True`): re-uploading an unchanged export is skipped on content hash; a new
  export (with more records) differs and is captured. Capture-once per Drive `file_id` (the
  sensor's `run_key`) is the first line of defense; content-hash dedup backstops a re-upload of the
  same file under a new id.
- **Drive provenance in the sidecar:** `file_id`, `file_name`, `folder_id`, and Drive
  `created_time` / `modified_time`. **Never** the service-account key or any credential.

## Soil / USCRN specifics

- **One collection** (`uscrn/hourly`); payloads are the **raw text rows** for a single UTC date,
  sliced out of the station's year file (`text/plain` → `ext=txt`). Pure selection — the stored
  lines are byte-faithful to the source (no value parsing/reformatting).
- **Why slice, not store the whole file.** The source is one ever-growing year file (one row/hour),
  so a whole-file content hash differs on every fetch and would never dedup — re-storing the year
  several times a day. Storing only the partition date's rows means a finished day is ~24 lines
  stored **once**; today re-writes only when a new hourly row appears.
- **`dt` is the partition's UTC date** (the asset passes its partition key), matched against the
  file's `UTC_DATE` column (field 2). Re-captures of a trailing day dedup against the right folder.
- **Dedup on** (`dedupe=True`): an unchanged day-slice across ticks is skipped; a day that gained a
  row differs and is captured (so today's folder may hold a few progressively-larger slices — each a
  faithful snapshot of what the source showed at that time).
- **Provenance in the sidecar:** `station`, `wbanno`, `year`, `utc_date`, `row_count`, and the
  source `request_url`. No secrets exist for this source (public NOAA HTTP).
