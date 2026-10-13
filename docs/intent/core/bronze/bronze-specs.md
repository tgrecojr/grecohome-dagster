# Bronze writer — EARS specs

Design: [bronze-design.md](bronze-design.md). Prefix `CORE-BRONZE`. All specs are about
`grecohome_core.bronze.capture_bronze`.

## Writing

- [x] **CORE-BRONZE-001**: When `capture_bronze` accepts a payload, the system shall write the payload bytes unchanged to `{bronze_root}/{source}/{collection}/dt={dt}/{collection}_{fetched_unix_ms}_{short_id}.{ext}` and write a `.meta.json` sidecar with the same name stem beside it.
- [x] **CORE-BRONZE-002**: When `capture_bronze` is called with a non-empty `dt`, the system shall write into the `dt=<dt>` folder; when `dt` is `None` or empty, the system shall use the UTC date of the fetch time.
- [x] **CORE-BRONZE-003**: The bronze writer shall use an explicit `ext` exactly as given; otherwise it shall derive the extension from `content_type` (parameters dropped, trimmed, lowercased: JSON types → `json`, CSV types → `csv`, XML types → `xml`, `text/plain` → `txt`, anything else → `bin`) and append `.gz` when `stored_encoding` is `gzip` in any case.
- [x] **CORE-BRONZE-004**: The bronze writer shall set the sidecar's `source`, `collection`, `fetched_at`, `fetched_at_unix_ms`, `byte_size` and `sha256` itself, overriding any caller value, default `stored_encoding` to `identity` and `schema_version` to `v1` when the caller gives none, and pass every other caller `meta` key through unchanged.
- [x] **CORE-BRONZE-005**: The bronze writer shall write each file atomically, to a hidden temporary file in the same directory that is renamed into place, and shall write the payload before its sidecar.

## Failure handling and guards

- [x] **CORE-BRONZE-006**: If any step of a bronze capture fails, then the system shall log a `bronze capture failed` WARNING with the source, collection and error, return `None`, and not raise.
- [x] **CORE-BRONZE-007**: If `capture_bronze` receives a payload that is not `bytes` or `bytearray`, then the system shall treat it as a capture failure (CORE-BRONZE-006) and write nothing.
- [x] **CORE-BRONZE-008**: If the caller's `meta` carries an integer `http_status` outside 200–299, then the bronze writer shall write nothing, log a `bronze capture skipped: non-2xx response` WARNING, and return `None`.

## Skip-identical

- [x] **CORE-BRONZE-009**: Where `dedupe` is true (the default), the bronze writer shall skip writing a payload whose sha256 equals the `sha256` in the newest sidecar (by fetch time in its filename) of the same `(source, collection, dt)` folder, and return `None`.
- [x] **CORE-BRONZE-010**: If, with `dedupe` true, the folder's newest sidecar is missing, unreadable or has no `sha256`, then the bronze writer shall write the payload.
- [x] **CORE-BRONZE-011**: Where `dedupe` is false, the bronze writer shall write every accepted payload, including one identical to an earlier capture.
- [x] **CORE-BRONZE-012**: When the bronze writer writes a payload, it shall return the payload's path.
