---
parent: core
prefix: CORE-BRONZE
---

# Bronze writer

## Context and Design Philosophy

Bronze is the raw, immutable record of exactly what each source returned. It is the only
**source of truth**: silver and gold are derived from it, rebuildable at any time, and never
modify it. Bronze exists so raw data is kept once, unaltered, and can be reprocessed later
without re-fetching from rate-limited or non-replayable sources.

One function writes it: `grecohome_core.bronze.capture_bronze(source, collection, raw_bytes,
meta, *, bronze_root, dt=None, dedupe=True, dedupe_scope="folder", ext=None)`, returning a
`CaptureResult`. Every subject captures through it.

## What to capture

Bronze holds a source's **functional domain data**: the records you would ever want to
reprocess. It is not a log of every byte exchanged with the source. The subject decides **per
endpoint or call** what counts as data; the writer doesn't infer it.

- **Capture:** domain records (sleep, recovery, workouts, glucose readings, temperature rows)
  and reference endpoints that give real context (profile, devices, units). Each reference
  endpoint gets **its own collection** (for example `whoop/profile`, `garmin/devices`).
- **Skip, because it's pointless:** protocol plumbing (keep-alives, health checks, `204`,
  redirects), empty pagination terminators, and handshake responses that carry no domain data.
- **Never capture, because it's a security rule:** OAuth token, refresh and authorization-grant
  responses; API keys, client secrets, bearer or session tokens and passwords, in any payload,
  header or field; signed URLs that embed credentials. Bronze is immutable and backed up, so a
  secret written there can't easily be purged. If a data payload echoes a secret, that field
  is removed before capture. This is the **one** permitted change to a payload, and the
  removed paths are recorded in the sidecar's `redacted_fields`. Secrets never go into the
  sidecar's `request_url` or `request_params` either.
- **Error responses are not captured.** When the caller records an `http_status`, only a 2xx
  response is written; a non-2xx is logged and skipped (see *Guards*). An error envelope stored
  as a payload poisons the content and integrity checks downstream. The subject logs the
  failure instead.

## Writing rules

- **Bytes only.** The payload is written byte-for-byte as received (`response.content`),
  never as a decoded string. Decoding passes data through character-set assumptions, and a
  mojibake bug at capture time would corrupt bronze irreversibly. Decoding happens downstream,
  where it can be redone. A non-`bytes` payload is rejected (see *Guards*).
- **Content encoding is resolved deliberately:**
  - **Transport compression** (a server gzipping JSON for the trip; HTTP clients decompress
    it transparently) is stored **decompressed**. The arrival encoding is recorded in
    `content_encoding`.
  - **Native compressed formats** (an export delivered as `.csv.gz`) are stored **as
    received**, with the extension reflecting it.
  - **Character set:** the declared charset is recorded in `charset`. Payloads are never
    decoded before storing.
  - **At-rest compression** is a separate choice that bronze doesn't make today
    (`stored_encoding: identity`). If it is ever applied, it must be uniform and recorded in
    `stored_encoding`.
- **Append-only and immutable.** Every written capture is a new, uniquely named file. The
  writer never edits or overwrites a file, and deletes one only to undo its own half-finished
  capture (below). Re-runs and restatements produce new files.
- **Never overwrite, even by accident.** Each file is moved into place with an **exclusive
  create** (it fails if the name exists). Two captures in the same millisecond with the same
  random `short_id` (about 1 in 16 million) would otherwise silently replace one another; on that
  collision the writer draws a new `short_id` and tries again, up to five times.
- **Atomic.** Each file is written to a hidden temporary file (`.tmp_<hex>`) in the same
  directory, flushed and `fsync`ed, then renamed into place. Readers' globs skip dotfiles, so
  a half-written file is never seen. The temporary file is removed when a write raises, but a
  hard crash can leave one behind. The directory itself isn't `fsync`ed.
- **A payload always has its sidecar.** The payload is written first, then the sidecar. If the
  sidecar write fails, the writer **deletes the payload it just wrote** and reports the capture as
  failed, so bronze never holds a payload without provenance: silver would read it, while
  skip-identical (which reads sidecars) couldn't see it. The next tick re-captures. Only a process
  killed between the two writes can still leave such an orphan.
- **Non-fatal.** `capture_bronze` never raises. Any failure is logged as a WARNING
  (`bronze capture failed`, with source, collection and error) and reported as a failed outcome
  (see *Return value*), so the caller's work continues. A successful write logs `bronze capture written` at DEBUG, with
  the path and byte size.
- **The root is passed in.** The caller supplies `bronze_root` (from `BRONZE_ROOT`); nothing is
  hardcoded, which keeps an object-store migration open. `BRONZE_ROOT` is required
  configuration for every subject: bronze is always on.

## Layout and naming

```
{bronze_root}/{source}/{collection}/dt={YYYY-MM-DD}/
    {collection}_{fetched_unix_ms}_{short_id}.{ext}
    {collection}_{fetched_unix_ms}_{short_id}.meta.json
```

| Segment | Meaning |
|---|---|
| `source`, `collection` | Lowercase provider and dataset names (`whoop`, `sleep`) |
| `dt=` | The **partition date** the caller passes (`dt`); if none is passed, the fetch-time UTC date |
| `fetched_unix_ms` | Capture time, Unix epoch milliseconds, UTC |
| `short_id` | 6 random hex characters (3 bytes), making a same-millisecond collision improbable but not impossible (see *Open Questions*) |
| `ext` | The **stored** form. An explicit `ext` from the caller is used exactly as given. Otherwise it's derived from `content_type` (parameters dropped, trimmed, lowercased): `application/json`/`text/json` → `json`, `text/csv`/`application/csv` → `csv`, `application/xml`/`text/xml` → `xml`, `text/plain` → `txt`, anything else → `bin`. `.gz` is appended when `stored_encoding` is `gzip` (any case) |

Names should use only lowercase letters, digits, underscores, hyphens and the `dt=` marker:
nothing awkward as an object-store key. The writer doesn't validate or normalize `source`,
`collection`, `dt` or `ext`; that's the caller's responsibility. An empty `dt` falls back to
the fetch date, like `None`. Callers pass their partition key as `dt`, so re-captures of a
trailing day land in, and are compared against, the right folder. `fetched_at` is formatted
`YYYY-MM-DDTHH:MM:SS.mmmZ`.

## Sidecar (`.meta.json`)

Every payload gets a sidecar with the provenance the bytes alone don't carry.

| Forced by the writer (override any caller value) | Defaulted by the writer (a caller value wins) | Passed through from the caller |
|---|---|---|
| `source`, `collection`, `fetched_at` (ISO UTC, ms), `fetched_at_unix_ms`, `byte_size`, `sha256` (of the stored bytes) | `stored_encoding` (`identity`), `schema_version` (`v1`) | `request_url`, `request_params` (enough to understand or replay the fetch, **never** secrets), `http_status`, `content_type`, `charset`, `content_encoding` (how it arrived), `processor`, `processor_version`, and source-specific fields such as `capture_mode` and `redacted_fields` |

`stored_encoding` is a label describing the bytes the caller handed over. The writer never
compresses anything itself.

`sha256` and `byte_size` describe the bytes actually on disk, so integrity can be verified
later. A non-empty `redacted_fields` flags that the stored payload isn't byte-identical to what
arrived, and names what was removed. The schema is open: extra source-specific keys are allowed.

## Skip-identical (`dedupe`)

With `dedupe=True` (the default), the writer computes the payload's sha256 and compares it with
the sha256 of the newest earlier capture **in the same `(source, collection, dt)` folder**. If
they're equal, nothing is written (a DEBUG `bronze capture deduped` line is logged) and the
outcome is *skipped as identical*. A source that re-captures an overlapping window then costs API
calls but no storage unless it returned something new. Which earlier capture counts depends on
the scope:

- **`dedupe_scope="folder"`** (the default): the newest sidecar in the folder, by the fetch time
  in its filename (a tie goes to whichever file the directory listing returns first, and an
  unparseable name counts as time 0). Right when **one request** writes the folder per run.
- **`dedupe_scope="request"`**: the newest sidecar in the folder **whose `request_params` equal
  this capture's** (compared as canonical JSON). Right when **several requests** write one folder
  (one per activity, device or goal status): each request is compared only with its own earlier
  captures, so two different requests returning identical bytes are both kept. A capture with no
  earlier sidecar for the same request is written. Existing sidecars already record
  `request_params`, so this works against bronze captured before it existed.

The rules that hold in both scopes:

- **Only identical bytes are skipped**, so skip-identical never loses data: the matching earlier
  capture already holds exactly those bytes.
- **It gives way when unsure.** If the matching sidecar is missing, unreadable or has no
  `sha256`, the payload is written.
- **Callers decide per call** (`dedupe=False` keeps every capture; the scope picks the
  comparison). Which paths use which is the subject's decision.

## Guards

The guards run in this order: type, then non-2xx, then skip-identical.

- **Type.** A `raw_bytes` that isn't `bytes`/`bytearray` is a capture failure (logged, outcome
  `failed`): bronze stores bytes, never decoded text.
- **Non-2xx.** If `meta["http_status"]` is an integer outside 200–299, nothing is written; a
  WARNING `bronze capture skipped: non-2xx response` is logged and the outcome is
  `skipped_non_2xx`.
  Callers that don't record a status (they only ever capture successes) are unaffected. A
  status passed as a string isn't checked, so callers pass an integer.

## Return value

`capture_bronze` returns a `CaptureResult` with an `outcome` and a `path`:

| `outcome` | When | `path` |
|---|---|---|
| `written` | The payload and its sidecar were written | The payload's path |
| `skipped_identical` | Skip-identical matched an earlier capture | None |
| `skipped_non_2xx` | The non-2xx guard refused the payload | None |
| `failed` | Any failure (including a rolled-back sidecar write) | None |

Callers can then tell a run that wrote nothing because nothing changed from one whose writes
failed, and report per-run counts.

## Non-goals

- No parsing, typing or transformation in bronze; that's silver.
- No deletion, retention or cleanup: bronze grows append-only.
- No object-store client yet. Local filesystem only, written so the root is swappable.

## Decisions & Alternatives

| Decision | Chosen | Alternatives Considered | Rationale |
|---|---|---|---|
| Error responses | Not captured (2xx only when a status is recorded) | Capture non-2xx bodies as diagnostic records | Stored error envelopes (for example a 401 "Authorization was not valid") poisoned the content and integrity checks. Logging the status explains gaps without polluting bronze. |
| Partition folder | The caller's partition date (`dt`), defaulting to the fetch date | Always the fetch date | Partition-dated folders let re-captures of a trailing day be compared within the right folder, which is what makes skip-identical work. Event-date semantics still belong to silver. |
| Capture when `BRONZE_ROOT` is unset | Not supported: `BRONZE_ROOT` is required, bronze is always on | An opt-in no-op mode when unset | The no-op mode existed to roll capture out to running apps one source at a time. Capture is now the subjects' whole purpose, so a silent no-op would only hide a misconfiguration. |
| Skip-identical scope | Per call: the newest capture in the folder, or the newest capture of the same request | Folder only; compare with every capture in the folder; a caller-supplied key | Folder scope is one cheap read when one request writes a folder. Request scope lets multi-request folders (Garmin's per-activity collections, about 38% of its bytes) skip identical re-captures; matching on the `request_params` sidecars already carry needs no caller-invented key and works on existing bronze. |
| Write order | Payload, then sidecar; delete the payload if the sidecar fails | Sidecar first; keep an orphan payload; hash the newest payload for skip-identical | A sidecar without its payload would make skip-identical skip data that isn't there. Rolling back keeps every payload paired with its provenance at the cost of one re-capture. |
| Overwrite protection | Exclusive create, retry with a new `short_id` | Plain rename | A same-millisecond id collision must never replace a capture. |
| Failure behavior | Log and report a `failed` outcome, never raise | Raise to the caller | Capture is a side effect of fetching. A failed write must not lose the fetched data's processing or fail the run. Callers that must not lose a capture (Lingo) act on the outcome. |
| Outcome reporting | A `CaptureResult` with an outcome | A path or `None` | `None` meant skipped, refused or failed alike, so a run that wrote nothing looked healthy. |

## Open Questions & Future Decisions

No open questions.

## References

- `packages/core/src/grecohome_core/bronze/capture.py`
- `docs/intent/garmin/capture/capture-design.md`, `docs/intent/whoop/capture/capture-design.md`: how each subject uses the writer
