# Bronze writer — EARS specs

Design: [bronze-design.md](bronze-design.md). Prefix `CORE-BRONZE`. All specs are about
`grecohome_core.bronze.capture_bronze`.

## Writing

- [x] **CORE-BRONZE-001**: When `capture_bronze` accepts a payload, the system shall write the payload bytes unchanged to `{bronze_root}/{source}/{collection}/dt={dt}/{collection}_{fetched_unix_ms}_{short_id}.{ext}` and write a `.meta.json` sidecar with the same name stem beside it.
- [x] **CORE-BRONZE-002**: When `capture_bronze` is called with a non-empty `dt`, the system shall write into the `dt=<dt>` folder; when `dt` is `None` or empty, the system shall use the UTC date of the fetch time.
- [x] **CORE-BRONZE-003**: The bronze writer shall use an explicit `ext` exactly as given; otherwise it shall derive the extension from `content_type` (parameters dropped, trimmed, lowercased: JSON types → `json`, CSV types → `csv`, XML types → `xml`, `text/plain` → `txt`, anything else → `bin`) and append `.gz` when `stored_encoding` is `gzip` in any case.
- [x] **CORE-BRONZE-004**: The bronze writer shall set the sidecar's `source`, `collection`, `fetched_at`, `fetched_at_unix_ms`, `byte_size` and `sha256` itself, overriding any caller value, default `stored_encoding` to `identity` and `schema_version` to `v1` when the caller gives none, and pass every other caller `meta` key through unchanged.
- [x] **CORE-BRONZE-005**: The bronze writer shall write each file atomically, to a hidden temporary file in the same directory that is moved into place, and shall write the payload before its sidecar.

## Failure handling and guards

- [x] **CORE-BRONZE-006**: If any step of a bronze capture fails, then the system shall log a `bronze capture failed` WARNING with the source, collection and error, return a result with outcome `failed` and no path, and not raise.
- [x] **CORE-BRONZE-007**: If `capture_bronze` receives a payload that is not `bytes` or `bytearray`, then the system shall treat it as a capture failure (CORE-BRONZE-006) and write nothing.
- [x] **CORE-BRONZE-008**: If the caller's `meta` carries an integer `http_status` outside 200–299, then the bronze writer shall write nothing, log a `bronze capture skipped: non-2xx response` WARNING, and return a result with outcome `skipped_non_2xx` and no path.

## Skip-identical

- [x] **CORE-BRONZE-009**: Where `dedupe` is true (the default) and `dedupe_scope` is `folder` (the default), the bronze writer shall skip writing a payload whose sha256 equals the `sha256` in the newest sidecar (by fetch time in its filename) of the same `(source, collection, dt)` folder, and return a result with outcome `skipped_identical` and no path.
- [x] **CORE-BRONZE-010**: If, with `dedupe` true, the sidecar the comparison selects is missing, unreadable or has no `sha256`, then the bronze writer shall write the payload.
- [x] **CORE-BRONZE-011**: Where `dedupe` is false, the bronze writer shall write every accepted payload, including one identical to an earlier capture.
- [x] **CORE-BRONZE-012**: When the bronze writer writes a payload and its sidecar, it shall return a result with outcome `written` and the payload's path.
- [x] **CORE-BRONZE-013**: Where `dedupe` is true and `dedupe_scope` is `request`, the bronze writer shall skip writing a payload whose sha256 equals the `sha256` in the newest sidecar (by fetch time in its filename) of the same `(source, collection, dt)` folder whose `request_params` equal the capture's `request_params`, and return a result with outcome `skipped_identical`; if no sidecar in the folder has the same `request_params`, it shall write the payload.

## Integrity

- [x] **CORE-BRONZE-014**: If writing a capture's sidecar fails after its payload was written, then the bronze writer shall delete that payload and return a result with outcome `failed`.
- [x] **CORE-BRONZE-015**: The bronze writer shall never replace an existing file: if a capture's payload or sidecar name already exists, it shall draw a new `short_id` and try again, up to five attempts, and report `failed` if every attempt collides.
