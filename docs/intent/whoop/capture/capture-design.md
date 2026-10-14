---
parent: whoop
prefix: WHOOP-CAPTURE
---

# Whoop capture

## Context

This component captures what Whoop returns to bronze: four daily range collections, re-captured
hourly across a trailing reconcile window because Whoop rescores and deletes records after the
fact, and two current-only snapshots. It obtains its bearer token from the
[token lifecycle](../auth/auth-design.md) and never retries a token failure.

## Collections, partitions and schedules

| Asset | Collections | Partitioning |
|---|---|---|
| `whoop_bronze_<collection>` | `sleep`, `recovery`, `workout`, `cycle` | Daily UTC fetch-slices from `2024-01-01`, `end_offset=1` (the current day is included) |
| `whoop_bronze_snapshots` | `profile`, `body_measurement` | Unpartitioned; captured into the fetch-date folder |

Two hourly schedules share the cron `17 * * * *` (UTC):

- **`whoop_hourly`** re-captures the trailing `RECONCILE_WINDOW_DAYS + 1` partitions (default
  8). The `run_key` is `<partition>-<YYYYMMDDTHH>`, so each tick is a distinct run.
- **`whoop_snapshots_hourly`** captures the snapshots, with `run_key`
  `snapshots-<YYYYMMDDTHH>`.

The trailing window guarantees that rescores are eventually captured; the hourly cadence
does not. All API-touching assets share the `whoop_api` concurrency pool, whose limit is
set on the host instance.

**Why minute 17.** The access token lives about one hour, the same as the poll cadence, so
every tick refreshes the token right at its expiry. Running at `:00` put that refresh inside
the internet-wide top-of-hour traffic surge, when Whoop's token endpoint is slowest and most
likely to return a 5xx in the middle of a rotation. Every grant loss seen while the schedules
ran at `:00` happened at `HH:00`. Since the schedules moved to `:17` (2026-07-24), no grant
has been lost in about 80 days; before that, five were lost between 2026-07-20 and 07-24. Both schedules share
the minute, so the one refresh they trigger is serialized by the token lock (see the [token lifecycle](../auth/auth-design.md)).

## Capture

### Range collections

| Collection | Endpoint | Completeness event date |
|---|---|---|
| `sleep` | `/developer/v2/activity/sleep` | record `start` |
| `recovery` | `/developer/v2/recovery` | record `created_at` |
| `workout` | `/developer/v2/activity/workout` | record `start` |
| `cycle` | `/developer/v2/cycle` | record `start` |

Each partition run sends that partition's **UTC day** as Whoop's time filter: `start` is
the partition's 00:00Z and `end` is the next day's 00:00Z, both with millisecond precision
(`YYYY-MM-DDTHH:MM:SS.mmmZ`). For the current, in-progress day, `end` is in the future and is
sent as is. Which records match is Whoop's own filter: a record that spans midnight (an
in-progress cycle, an overnight sleep) can come back for **both** days, so it's captured in
two partitions. Silver deduplicates by record id.

Every successful page is captured to `BRONZE_ROOT/whoop/<collection>/dt=<partition>/` with
skip-identical on. The run records the number of records fetched (`records`), the `partition`,
and its capture outcomes (`written`, `skipped_identical`, `failed`: counts across the run's pages)
as materialization metadata.

What is *not* captured:
- **Pages with zero records**: the empty end-of-results page, or a window with no records.
  So a collection writes nothing for a day with no records (common for `workout`).
- **Error responses.** Only a successful (2xx) response body is captured; for an error, only
  its status is logged. The client checks this, and the core writer refuses non-2xx too.
- **A 2xx body that isn't JSON.** It raises a non-retryable `WhoopAPIError` and is not
  captured.

Capture is **best-effort**. A failed bronze write (a full disk, permissions) logs a WARNING
and the run still succeeds, reporting the records it fetched. Freshness is what detects
captures that stop landing.

How much each collection writes follows from how often Whoop changes it. The in-progress
`cycle` changes every hour (strain accumulates), so the current day is written about hourly.
`sleep` and `recovery` change only on rescores, so most re-captures are skipped as identical.

**Known limitation.** Skip-identical compares against the newest file in the day folder, so
a day whose records span **more than one page** (more than 25 records) is rewritten on every
re-capture. Real Whoop days fit in one page, so this costs nothing in practice.

### Snapshots

| Collection | Endpoint |
|---|---|
| `profile` | `/developer/v2/user/profile/basic` |
| `body_measurement` | `/developer/v2/user/measurement/body` |

Both are single objects, not record lists, so they are always captured, into the **UTC
fetch-date** folder with skip-identical on. Each day's first capture lands in a new, empty
folder and is written; identical captures later that day are skipped. So each snapshot is
written about once a day (at the first tick after 00:00 UTC), and again within a day only if
it changes.

One asset fetches both, **profile first, then body measurement**:
- If the body-measurement call fails, the profile is already captured and the run fails.
  Only `body_measurement` freshness then goes stale.
- If the profile call fails, body measurement isn't attempted.

The asset's metadata (`profile: true`, `body_measurement: true`) means both calls returned.
It doesn't confirm that a file was written; freshness covers that.

### Data requests

- **Timeout:** 30 s per request. One HTTP client per `WhoopClient`, reused across requests.
- **Rate limit:** each `WhoopClient` (one per asset step) has its own limiter, at 90% of
  `MAX_REQUESTS_PER_MINUTE` (default 60, so 54 requests a minute). Every attempt, retries
  included, counts against it. It doesn't coordinate across steps or runs; the host's
  `whoop_api` pool bounds that.
- **Retry:** up to 3 attempts on a timeout, a network error, a 429, or a 500/502/503/504.
  - A server `Retry-After` in seconds is honored (for 429 and 5xx), capped at 60 s. The
    HTTP-date form is ignored.
  - Otherwise attempts are 2 s apart. The backoff is exponential with a 2 s floor and a 10 s
    ceiling, but at 3 attempts it never climbs past the floor.
  - Once attempts run out, the last error is raised: `WhoopRetryableError` for a status, or
    the httpx timeout/network error.
  - Any other error status raises `WhoopAPIError` without retry. So does a protocol error
    that isn't a network error, such as `RemoteProtocolError`.
  - A failure to obtain the access token raises `WhoopAuthError`, which this retry never
    matches (see *Retry rule* under the token lifecycle).

## Pagination

Range endpoints are cursor-paginated:

- Each request asks for **25 records**, Whoop's maximum (a larger limit is clamped to 25).
- Each response's `next_token` is sent back as `nextToken` on the next request.
- Pagination ends normally when a response has no `next_token`, or has zero records (even
  if it still carries a `next_token`).

Whoop sometimes returns a `next_token` that never clears. Two guards stop the loop so the
run finishes and releases the `whoop_api` pool instead of wedging every later tick:

1. **Cursor didn't advance.** A response whose `next_token` was already seen in this
   pagination stops it, with a WARNING. That catches a token that repeats immediately (`A,A`)
   or cycles (`A,B,A`). A seen token is never sent again, so no page is fetched twice.
2. **Page cap.** Fetching **1000 pages** stops it, with a WARNING.

When a guard stops pagination, the records fetched so far, including those on the page that
returned the repeated token, are returned and their pages captured. The run succeeds: a
stuck cursor is a Whoop-side fault, and the next tick re-captures the window.

If a page **request fails** (after its retries), the pages before it are already captured
and the run fails.

## Bronze checks

The collection checks are built by the shared core builders from one `CollectionCheckConfig`
per collection. They read bronze only (no Whoop API call, no `whoop_api` pool) and run hourly
at `:00` in `whoop_bronze_checks_job`, alongside `whoop_token_health`. Each looks at the 14
most recent **existing** `dt=` folders of its collection. For a sparse collection like
`workout`, that window spans weeks, not 14 days.

| Collection | Freshness (ERROR) | Completeness (WARN) | Schema drift (ERROR) | Content health |
|---|---|---|---|---|
| `sleep`, `recovery`, `cycle` | newest capture within 26 h + 6 h grace | event-date gap over 2 days | yes | yes |
| `workout` | **off** | event-date gap over **14 days** | yes | **off** |
| `profile`, `body_measurement` | newest capture within 26 h + 6 h grace | **off** (no event timeline) | yes | yes |

- **Content health** samples 5 payloads spread across those folders. It WARNs if **any**
  sampled payload is empty or error-like, and ERRORs on any integrity failure (on-disk bytes
  that don't match the sidecar).
- **Schema drift** records a baseline the first time it sees a collection and ERRORs when
  the schema changes from it. With `BRONZE_MONITOR_DIR` unset, it passes as "disabled".
- **Alerting:** freshness, schema drift and content health post to Slack on failure.
  Completeness gaps (WARN) show in Dagster only.

- **Workouts** write only on days with a workout, so freshness would measure "time since the
  last workout", not whether the poller is alive. It would raise an ERROR on every ordinary
  rest day. Poller liveness is already covered by the daily collections' freshness. Content
  health is off for the same reason: a sparse collection would keep tripping it.
- **Snapshots** are written about daily (see *Snapshots*), so freshness detects a stalled
  snapshots job.

## Decisions & Alternatives

| Decision | Chosen | Alternatives Considered | Rationale |
|---|---|---|---|
| Schedule minute | `:17` for both Whoop schedules | `:00`; staggering the two schedules apart | Grant losses clustered at `HH:00`; none in about 80 days since the move. Staggering the schedules apart doesn't reduce token-endpoint load, because the lock and re-read already collapse them into one refresh per hour. |
| Poll cadence | Hourly | Every 2–3 h | Hourly keeps intraday freshness. A slower cadence halves refresh exposure, but it is only worth taking if grant losses come back at `:17`. |
| Range request window | The partition's UTC day | Local days; a rolling window | Bronze partitions are UTC fetch-slices (HLD). Local-day semantics belong in silver. |
| Empty pages | Not captured | Capture every response | An empty page is the end-of-results marker or an empty window. Capturing it would add a file per day per collection with no information. |
| Runaway cursor | Stop with a WARNING and keep the partial results | Fail the run | Failing gains nothing: the next tick re-captures the window. Stopping releases the `whoop_api` pool, so later ticks aren't stuck behind the run. |
| Workout freshness and content health | Off; completeness at 14 days (WARN) | Freshness with a long tolerance | Workouts are written only on workout days, so freshness can't tell a stalled poller from a rest week. Daily-collection freshness covers liveness. |
| Snapshot freshness | On (26 h + 6 h) | Off | Each snapshot is written about daily (the first capture in each fetch-date folder), so freshness reliably detects a stalled snapshots job. |

## Open Questions & Future Decisions

### Deferred

1. **Cadence relaxation.** Revisit only if grant losses recur at `:17` (see the token
   lifecycle's open questions).
2. **Multi-page skip-identical.** If Whoop days ever exceed one page, per-request
   skip-identical in the core writer (issue #176) would fix the rewrites.

## References

- `docs/high-level-design.md`: bronze re-capture, skip-identical
- `packages/core/src/grecohome_core/checks/`: the shared check builders
