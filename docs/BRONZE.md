# Bronze layer

The bronze layer is the raw, immutable record of exactly what each source API returned. It is
the only thing the pipelines persist in this phase; silver/gold read from it later.

## Invariants

- **Bytes only.** Payloads are written byte-for-byte from `response.content`, never a decoded
  string. Decoding is a downstream concern.
- **Append-only / immutable.** Every written capture is a new, uniquely named file. Nothing is
  overwritten or deleted by the capture path.
- **Content-hash deduped.** Before writing, the payload's sha256 is compared to the newest
  capture for the same `(source, collection, dt)` partition; an identical payload is skipped.
  Re-capturing an overlapping window each hour therefore costs API calls but ~zero storage.
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
- `{ext}` reflects the *stored* form (`json` for Whoop; `bin` for unknown content types).

Example:

```
bronze/whoop/recovery/dt=2026-06-08/recovery_1717900000000_a1b2c3.json
bronze/whoop/recovery/dt=2026-06-08/recovery_1717900000000_a1b2c3.meta.json
```

## Sidecar (`.meta.json`)

Provenance for each payload. Filled by the capture function: `source`, `collection`,
`fetched_at`, `fetched_at_unix_ms`, `byte_size`, `sha256`, `stored_encoding`, `schema_version`
(`v1`). Passed through by the caller: `request_url`, `request_params`, `http_status`,
`content_type`, `charset`, `content_encoding`, `processor`, `processor_version`. **Never contains
secrets** (no auth headers/tokens).

## Whoop specifics

- Captured inside `_make_request`: successful payloads (skipping empty pagination terminators)
  **and** error bodies (with their real HTTP status, before `raise_for_status`) as diagnostics.
- Collections: `sleep`, `recovery`, `workout`, `cycle` (daily-partitioned), plus `profile` and
  `body_measurement` (current-only snapshots, fetch-date folder).
- Timestamps are stored as-received (UTC); no timezone conversion happens in bronze.
