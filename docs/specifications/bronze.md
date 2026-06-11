# Bronze Layer Specification

**Purpose:** Define how every data processor captures its raw input to a shared "bronze" layer, so raw data is preserved once, immutably, and can be reprocessed later without re-fetching from rate-limited or non-replayable sources (Whoop, Garmin, CGM, soil temperature, etc.).

**Scope of this change:** Add raw capture only. Do **not** alter how any processor currently parses, transforms, or serves data. Bronze is a side-effect added alongside existing logic, never a replacement for it.

---

## What to capture vs. exclude

Bronze captures the source's **functional domain data** — the records you would ever want to reprocess. It is **not** a log of every byte the processor exchanges with the source. The decision is made **per endpoint/call by the processor author**, not inferred at write time: mark which calls are data calls (capture) versus auth or plumbing (do not capture). When in doubt about whether something is data, the security rule below still applies absolutely.

### Never capture (hard rule — security)

These must **never** be written to bronze under any circumstances. Bronze is immutable, append-only, and replicated to backups — exactly the wrong place for a secret, because you cannot easily rotate or purge it out later.

- OAuth token, refresh, and authorization-grant responses (anything returning an access token, refresh token, or authorization code).
- API keys, client secrets, bearer tokens, session tokens, or passwords — in any payload, header, or field.
- Signed URLs or pre-signed links that embed credentials or tokens.
- Any response where a secret rides along with otherwise-useful data — if a data payload echoes back an auth token or key, that field must be removed before writing (this is the **one** permitted modification to a payload, and it should be rare; prefer choosing endpoints that don't return secrets).

This also means: never put auth headers, tokens, or query-string secrets into the sidecar's `request_url` / `request_params` (already stated in the sidecar rules — reaffirmed here).

### Skip (guidance — no functional value)

Safe to store, but pointless, so don't — keeps bronze honest about what it contains:

- Protocol plumbing: keep-alive pings, health checks, `204 No Content`, redirect bodies, CORS preflight.
- Empty pagination terminators (the "no more results" page that carries zero records). Pages that **do** carry records are data — capture them.
- Handshake or capability-negotiation responses that carry no domain data.

### Capture (the actual point)

- The source's domain records: recovery, sleep, workouts, glucose readings, temperature samples, etc. — the time-series and event data you'd reprocess.
- Reference / metadata endpoints that provide genuine context even though they aren't the main time-series: user profile, device list, units, timezone, sensor calibration. Capture these into their **own `collection`** (e.g. `whoop/profile/`, `garmin/devices/`) so they're separate from the event streams.

### Edge cases — handle these consistently

- **Error responses with a meaningful body** (e.g. `429`, or a `400` explaining a bad request): **capture them.** A non-2xx response with content is functionally valuable — six months out it explains *why* there's a gap in your data. Record the real HTTP status in the sidecar so the downstream (silver) layer knows to treat it as a diagnostic record, not parse it as data. Do **not** capture error responses that are empty or that carry credential-bearing content (the security rule wins).
- **Partial / mixed responses**: if a single response contains both data and a credential field, the security rule takes precedence — strip the credential, capture the rest, and note in the sidecar that a field was removed.



1. **Store the payload as bytes, never as a decoded string.** Write the unparsed, unmodified body — no reshaping, no pretty-printing, no field filtering, no parsing. (The single narrow exception is removing a credential that rides along in a data payload — see "What to capture vs. exclude." That aside, the payload is untouched.) Capture the raw body as bytes (e.g. `response.content`), **never** a decoded text string (e.g. `response.text`). Decoding a response to a string passes it through the runtime's character-encoding assumptions; a mojibake bug there silently and irreversibly corrupts bronze because the original bytes are gone. Let any text decoding happen downstream, where it is reversible. The whole value of bronze is that it is the untouched source of truth.
2. **Resolve content-encoding explicitly (see the dedicated section below).** "Exact bytes" is ambiguous for compressed responses because most HTTP clients transparently decompress a transport `Content-Encoding` before you see the body. The rule: store the **decompressed payload** when compression was a transport optimization, store the bytes **as-received** when compression is the payload's native file format, and always record both the arrival encoding and the stored state in the sidecar. Do not conflate this with at-rest compression, which is a separate storage choice.
3. **Append-only and immutable.** Never edit, overwrite, or delete a bronze file after it is written. Each capture is a new file.
4. **Non-fatal.** A failed bronze write must never break or block the processor's existing work. Catch and log the failure; let normal processing continue.
5. **Capture before processing.** Write the raw bytes to bronze first, then run the existing processing. If the process crashes mid-run, the raw data is already safely captured.
6. **Disabled by default.** If `BRONZE_ROOT` is unset or empty, capture is a complete noop and the processor behaves exactly as before (see Storage target). Capture is opt-in per environment.

---

## Content encoding (read carefully — this is the easy thing to get wrong)

There are three distinct "encoding" concerns. Handle each explicitly rather than relying on "store what I received," which is ambiguous.

### 1. Transfer-Encoding (e.g. `chunked`) — ignore it

This is hop-by-hop wire framing. The HTTP client strips it before you ever see the body. It is not part of the data; do not attempt to preserve it.

### 2. Content-Encoding (e.g. `gzip`, `br`, `deflate`, `identity`) — decide deliberately

Most HTTP clients (`requests`, `httpx`, `fetch`) **transparently decompress** a transport `Content-Encoding`, so the body in your hands is usually already decompressed even though compressed bytes crossed the wire. Two cases:

- **Compression was a transport optimization** (the server gzipped an otherwise-JSON response for the trip): **store the decompressed payload.** The compression is an artifact of the connection, not of the data's meaning. Storing decompressed means DuckDB, Polars, `cat`, or your eyes can open the file with no extra step, and no future reader risks not knowing it must gunzip first. Record `content_encoding: "gzip"` in the sidecar so the provenance is preserved. Extension reflects the **stored** form (`.json`, not `.json.gz`).
- **Compression is the payload's native file format** (the source hands you a `.csv.gz` export as a deliberate format, common with some CGM and Garmin bulk dumps): **store the bytes as-received.** Here the gzip *is* the data's shape. Extension reflects that (`.csv.gz`), and `content_encoding` notes it arrived that way.

The test: was it compressed *for transport* (decompress and store) or compressed *as its format* (store as-is)?

### 3. Character encoding (charset, e.g. UTF-8) — never decode before storing

Text payloads carry a character encoding declared in `Content-Type; charset=...` or an XML/JSON prolog. Capture bytes, not a decoded string (see principle 1). Record the declared charset in the sidecar; let the decode happen downstream where it is reversible.

### At-rest compression is a *separate* decision

Whether bronze is compressed *on disk* is a storage choice independent of HTTP content-encoding, and you will likely want it (health-data JSON often compresses 5–10×). If you apply it, do so **uniformly and as your own layer** — not as a passthrough of whatever the server happened to send — and record it in the sidecar's `stored_encoding` field so a reader knows the difference between "arrived gzipped" and "we gzipped it at rest." For v1 you may skip at-rest compression entirely; just keep the field accurate.



For now, bronze is a **local filesystem directory**. The implementation must keep the S3 migration path open by treating the bronze root as a single configurable base location and never assuming anything beyond "a place to put objects at a key/path."

- Bronze root is supplied via an environment variable: `BRONZE_ROOT` (e.g. `/data/bronze`).
- All paths below are relative to `BRONZE_ROOT`.
- Do **not** hardcode the root anywhere. A future change will repoint `BRONZE_ROOT` (or swap in an S3 client) without touching capture logic.

### Disabled by default — noop when `BRONZE_ROOT` is unset

Bronze capture is **opt-in per environment**. If `BRONZE_ROOT` is unset or empty, capture is a complete noop: the processor behaves exactly as it did before this change. This is the migration safety guarantee — the capture code can be merged and shipped to every project at once, the apps confirmed to behave identically, and capture switched on one source at a time by setting the variable.

Rules:

- **Single early guard.** Check for `BRONZE_ROOT` at the very start of the capture function, before any path computation, hashing, timestamping, or directory creation. If it's not usable, return immediately with no side effects of any kind.
- **Unset and empty are both "off."** Treat a missing variable and an empty string (`""`) identically as disabled. Never fall back to the current working directory, a temp dir, or any default path — an unset root means *do nothing*, not *write somewhere*.
- **No directory creation when disabled.** The noop must not create `BRONZE_ROOT` or any subdirectory as a side effect.
- **Silent at normal log levels.** "Disabled" is a valid intended state, not a failure. Do not warn on every call. A single debug/info line at startup (e.g. "bronze capture disabled: BRONZE_ROOT not set") is fine; a per-call warning is not. This is distinct from the *failure* path (a set root that fails to write), which remains a warning.

---

## Path and naming standard

```
{BRONZE_ROOT}/{source}/{collection}/dt={YYYY-MM-DD}/{collection}_{fetched_at_unix_ms}_{short_id}.{ext}
```

### Components

| Segment | Definition | Example |
|---|---|---|
| `source` | The data provider, lowercase, no spaces | `whoop`, `garmin`, `cgm`, `soil` |
| `collection` | The specific dataset within the source, lowercase, underscores for spaces | `recovery`, `sleep`, `workout`, `glucose`, `temperature` |
| `dt=YYYY-MM-DD` | Date partition based on **fetch time in UTC** (the day the data was captured, not the day the data is about) | `dt=2026-06-04` |
| `fetched_at_unix_ms` | Capture timestamp, Unix epoch in **milliseconds**, UTC | `1717459200000` |
| `short_id` | A short random suffix (e.g. 6–8 hex chars) to guarantee uniqueness for captures within the same millisecond | `a3f9c1` |
| `ext` | The true content type of the **stored** bytes (reflects stored form, not arrival form — see Content encoding) | `json`, `json.gz`, `csv`, `xml`, `bin` |

### Example keys

```
whoop/recovery/dt=2026-06-04/recovery_1717459200000_a3f9c1.json
garmin/sleep/dt=2026-06-04/sleep_1717459251337_7b2e90.json
cgm/glucose/dt=2026-06-04/glucose_1717459260000_c14d8a.json
soil/temperature/dt=2026-06-04/temperature_1717459275512_f0a221.json.gz
```

### Rules

- **Never reuse a filename.** No `latest`, no fixed names, no overwriting. Restatements and re-runs each produce a new file. (Sources like Whoop and Garmin re-score historical records; we want every version on disk.)
- **Partition by fetch date, not event date.** Keeps writes simple and append-only; event-date organization belongs to a later (silver) layer.
- Filenames and directory names use only lowercase letters, digits, underscores, hyphens, and the `dt=` partition marker. No spaces, no characters that are awkward as S3 keys.

---

## What to write

For each capture, write **two things**:

1. **The payload file** — the exact raw bytes, at the path above.
2. **A sidecar metadata file** — same path with `.meta.json` appended, capturing the provenance the raw bytes alone don't carry.

### Sidecar metadata (`.meta.json`)

```json
{
  "source": "whoop",
  "collection": "recovery",
  "fetched_at": "2026-06-04T08:00:00.000Z",
  "fetched_at_unix_ms": 1717459200000,
  "request_url": "https://api.prod.whoop.com/developer/v1/recovery?limit=25",
  "request_params": { "limit": 25, "start": "2026-06-03T00:00:00Z" },
  "http_status": 200,
  "content_type": "application/json",
  "charset": "utf-8",
  "content_encoding": "gzip",
  "stored_encoding": "identity",
  "byte_size": 18423,
  "sha256": "e3b0c44298fc1c149afbf4c8996fb924...",
  "redacted_fields": [],
  "processor": "whoop-ingest",
  "processor_version": "1.4.2",
  "schema_version": "v1"
}
```

- `request_url` / `request_params`: enough to understand or replay the fetch. **Do not** include secrets, tokens, API keys, or auth headers in the sidecar.
- `sha256`: hash of the stored payload bytes (whatever is actually on disk), so integrity can be verified later.
- `content_encoding`: how the payload **arrived** over the wire (`gzip`, `br`, `identity`, etc.). Records provenance even when the client decompressed it transparently.
- `stored_encoding`: how the bytes are **actually stored on disk** (`identity` if decompressed, `gzip` if you applied at-rest compression or stored a native `.gz` as-received). A reader uses this to know whether to decompress before opening; `content_encoding` alone is not enough.
- `charset`: declared character encoding for text payloads (e.g. `utf-8`), for reversible decoding downstream. Omit or `null` for binary.
- `byte_size`: size of the stored bytes, matching `stored_encoding`.
- `redacted_fields`: list of any field paths removed from the payload before writing (normally empty `[]`). Used only for the narrow case where a credential rode along in a data payload and had to be stripped. If non-empty, it flags that the stored payload is not byte-identical to what arrived, and names exactly what was removed.
- Keep the schema flexible — extra source-specific fields are fine, but the keys above should always be present.

---

## Write procedure (apply in each processor)

1. **Check `BRONZE_ROOT` first.** If unset or empty, return immediately — complete noop, no side effects (see Disabled by default). Everything below runs only when capture is enabled.
2. Receive the raw response from the source (HTTP body, file read, device payload, etc.). Capture the body as **bytes**, never as a decoded string.
3. **Resolve encoding (see Content encoding section):** if the client transparently decompressed a transport `Content-Encoding`, the bytes are already decompressed — store them as-is and record `content_encoding` (arrival) and `stored_encoding: "identity"`. If the payload is a native compressed file format, store as-received and set both fields accordingly. Pick the `ext` to match the stored form.
4. Compute `fetched_at` (UTC, millisecond precision), derive the `dt=` partition and filename.
5. **Write atomically:** write the payload to a temp file in the same directory, then rename into place. This prevents a half-written file from ever appearing under the final name. (Atomic rename works on local FS now and maps cleanly to S3 put-once semantics later.)
6. Write the `.meta.json` sidecar the same way.
7. Wrap the entire capture in error handling: on any failure, log a warning with enough context to diagnose (source, collection, error) and **continue** with normal processing. Never raise out of the capture path.
8. Then run the processor's existing logic, unchanged.

### Pseudocode

```
def capture_bronze(source, collection, raw_bytes, meta):
    # raw_bytes MUST be bytes (e.g. response.content), never a decoded string.
    root = env.get("BRONZE_ROOT")
    if not root:                      # unset OR empty string -> disabled
        return                        # complete noop: no paths, no dirs, no side effects
    try:
        fetched_ms = now_utc_unix_ms()
        dt         = utc_date_string(fetched_ms)          # YYYY-MM-DD
        short_id   = random_hex(6)
        ext        = ext_for(meta.content_type, meta.stored_encoding)  # stored form
        base       = f"{source}/{collection}/dt={dt}/{collection}_{fetched_ms}_{short_id}"

        write_atomic(f"{root}/{base}.{ext}", raw_bytes)              # bytes, as stored
        write_atomic(f"{root}/{base}.meta.json",
                     json_bytes(meta | {"sha256": sha256(raw_bytes),
                                        "byte_size": len(raw_bytes),
                                        "fetched_at_unix_ms": fetched_ms}))
    except Exception as e:
        log.warning(f"bronze capture failed for {source}/{collection}: {e}")
        # do not re-raise — capture is best-effort and must not break processing

# in the existing processor, before normal work:
# pass response.content (bytes), and a meta dict recording arrival vs stored encoding:
capture_bronze("whoop", "recovery", response.content, {
    "content_type":     "application/json",
    "charset":          "utf-8",
    "content_encoding": response.headers.get("Content-Encoding", "identity"),  # arrival
    "stored_encoding":  "identity",   # client already decompressed; bytes stored as-is
    # ... request_url, request_params (no secrets), http_status, processor, etc.
})
process_as_before(response)   # unchanged
```

---

## Explicit non-goals (do not build these now)

- No parsing, typing, deduplication, or transformation of bronze data.
- No database, query engine, or table format (Parquet/Delta/Iceberg) yet.
- No orchestrator, scheduler changes, or pipeline framework.
- No S3 client yet — local filesystem only, but written so the root is swappable.
- No deletion or retention/cleanup logic. Bronze grows append-only; revisit retention later.

---

## Definition of done (per project)

- [ ] Processor reads `BRONZE_ROOT` from environment; nothing hardcoded.
- [ ] With `BRONZE_ROOT` unset or empty, capture is a complete noop: no files, no directories, no behavior change, no per-call warnings — verified by running the app with the variable unset.
- [ ] Each call is classified as data / auth / plumbing; only data calls are captured.
- [ ] No OAuth, token, credential, or secret-bearing response is ever written to bronze.
- [ ] Reference/metadata endpoints (profile, devices, units) captured into their own collection if useful.
- [ ] Error responses with a meaningful body are captured with the real HTTP status in the sidecar; empty or secret-bearing errors are not.
- [ ] Raw response is written byte-for-byte as **bytes** (e.g. `response.content`), never a decoded string (`response.text`).
- [ ] Content-encoding is resolved deliberately: transport compression stored decompressed, native compressed formats stored as-received; `ext` matches the stored form.
- [ ] Sidecar records both `content_encoding` (arrival) and `stored_encoding` (on disk), plus `charset` for text payloads.
- [ ] Path and filename follow the naming standard exactly, with a unique timestamped name.
- [ ] A `.meta.json` sidecar is written alongside each payload, with no secrets.
- [ ] Writes are atomic (temp-file + rename).
- [ ] Capture failures are logged and non-fatal; existing processing is byte-for-byte unchanged in behavior.
- [ ] A manual test run produces files at the expected paths, and the stored bytes match the source response (verify the sha256).