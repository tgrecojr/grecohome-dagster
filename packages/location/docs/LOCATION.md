# Location subject

Promotes the external `locationrelay` service's raw staging files into bronze. This note records
the design choices and deferrals specific to this subject; the bronze contract itself is in
[../../docs/BRONZE.md](../../docs/BRONZE.md).

## Source → bronze

```
iPhone (Overland / OwnTracks)
   │ HTTPS POST (Bearer)
   ▼
locationrelay (Rust)  ── auth → validate → write RAW body byte-for-byte, one file/POST
   ▼                     (also forwards the parsed form to Dawarich in real time)
RELAY_CAPTURE_DIR/{overland|owntracks}/dt=YYYY-MM-DD/{received_unix_ms}_{shortid}.json
   │ scheduled promote asset (promoted-set in LOCATION_STATE_DIR)
   ▼
BRONZE_ROOT/location/{overland|owntracks}/dt=YYYY-MM-DD/{collection}_{fetched_ms}_{id}.json (+ .meta.json)
```

## Bronze mapping

| Field | Value |
|---|---|
| `source` | `location` |
| `collection` | `overland` \| `owntracks` (from the staging subdir) |
| `dt` | UTC **receipt** date from the staging path `dt=YYYY-MM-DD` (server-derived, trusted) |
| payload | the staging file bytes, **verbatim** (byte-exact raw POST body) |
| `ext` | `json` (passed explicitly) |
| `dedupe` | `False` — the per-file promoted-set is the capture-once guard |
| `capture_mode` | `raw` (byte-exact; recorded in the sidecar) |

- **`dt` = receipt date**, Lingo-style. True event time is *inside* each record
  (`properties.timestamp` for Overland points, `tst` for OwnTracks); receipt time is `received_at` /
  `received_unix_ms` in the sidecar (parsed from the staging filename, never in the payload).
- **`fetched_at` = promote time** (writer-stamped), distinct from `received_at`.
- **`dedupe=False`** + per-file promoted-set (never content-dedup) so two distinct byte-identical
  POSTs (e.g. a re-sent OwnTracks ping) both land.

## Idempotency / crash-safety

- **Primary guard:** the promoted-set (per stream, in `LOCATION_STATE_DIR`), keyed by the unique
  staging **filename**.
- **Durable backstop / rebuild key:** the `staging_file` recorded in each bronze sidecar. A staging
  file is "already promoted" iff a sidecar in its `dt` partition carries a matching `staging_file` —
  so the crash window (bronze written, promoted-set not advanced) is exactly-once, and a lost
  promoted-set rebuilds itself from bronze. This does not rely on the core writer's ambiguous `None`.
- The bronze backstop is consulted only for staging files *not* already in the promoted-set, so the
  steady state never re-scans days of sidecars.

## Checks

- **Content health** (WARN) — both streams; payloads parse and carry data.
- **Schema drift** (ERROR) — **overland only** (stable `["locations"]` signature). Skipped for
  OwnTracks: its messages are polymorphic (`_type` = location/transition/lwt, optional keys), which
  would churn false ERRORs on the richest-payload signature.
- **Receipt freshness** (WARN → ERROR) — hours since the newest `received_unix_ms` in bronze. WARN
  wide (`LOCATION_FRESHNESS_WARN_HOURS`, default 24h), ERROR only past a long gap
  (`LOCATION_FRESHNESS_ERROR_HOURS`, default 168h). Location is event-driven, so most gaps are
  legitimate (phone off / travel / stationary batching). A stream that has **never** captured (e.g.
  only OwnTracks is configured, so Overland has no receipts) is treated as *unused, not stale* — it
  passes green until data first flows, so an inactive stream never pages (a genuinely mis-mounted
  stream is caught by promote-lag, not freshness).
- **Promote lag** (ERROR) — no staging file older than `LOCATION_PROMOTE_LAG_HOURS` (default 6h)
  remains un-promoted; the early guardrail that the promoter keeps up before relay retention prunes
  staging.

## Deferrals (v1 non-goals)

- **~~No silver/gold in v1~~ — superseded.** `silver_location` now exists, enriched with
  reverse-geocoded place from the separate **`geocode`** bronze cache (Photon `/reverse`). The
  promoter itself stays pure (no source API, no enrichment) — the geocoding lives in its own subject.
  See [../../packages/geocode/docs/GEOCODE.md](../../geocode/docs/GEOCODE.md), `docs/SILVER.md`, and
  [ADR 0012](../../../docs/adr/0012-geocode-cache-silver-location.md). Gold place marts (time-at-place,
  home/away) remain deferred.
- **No forwarding in Dagster.** Forwarding to Dawarich stays in the relay's Rust worker (real-time);
  a Dagster reconcile-from-bronze forwarder is a future option only if the relay's in-memory forward
  queue is ever observed dropping data. Don't run both.
- **Clean split:** the relay never writes the lake, and this subject never writes the relay dir.
- **No changes to locationrelay** for v1 (its raw-capture change is already shipped).
