---
parent: high-level-design
prefix: GARMIN
---

# Garmin bronze subject

## Context and Design Philosophy

`grecohome_garmin` is the bronze code location for Garmin Connect. It pulls every allowlisted
catalog endpoint through the `garminconnect` library and writes what Garmin returned to
`BRONZE_ROOT/garmin/<collection>/dt=<YYYY-MM-DD>/` through the shared core writer
(`grecohome_core.bronze.capture_bronze`). Silver reads these files. Nothing in this package
transforms or interprets the data.

Garmin data for a given day is **not final when the day ends**. The watch syncs late, Garmin
computes some daily aggregates hours or days later, and summaries are restated as more data
arrives. In bronze, the second capture of a day differs from the first for most of the
important daily collections (`user_summary`, `spo2`, `respiration`, `steps_intraday`,
`stress`, `body_battery`, `sleep`). Some collections keep changing through the sixth or
seventh capture. The design therefore follows the HLD tenet *Completeness over economy*:
recent days are **re-captured every day** for a trailing window, and every capture that
differs from what bronze already holds is kept.

Storage is controlled by the core writer's **skip-identical** check rather than by capturing
less. This follows the HLD tenet *Shared core, source-specific overrides*: Garmin uses the
same re-capture-plus-skip-identical policy as Whoop and USCRN. The Garmin-specific override is
which captures may be skipped, described below.

## Collections and partitioning

Every catalog endpoint becomes one Dagster asset, `garmin_bronze_<collection>`, built by a
factory over `catalog.CATALOG`.

| Asset group | Endpoint kinds | Partitioning | Bronze `dt` folder |
|---|---|---|---|
| `garmin_daily`, `garmin_activities` | `daily`, `range`, `per_device_range` | Daily partitions from `2024-01-01`, `end_offset=0` (only completed days) | The partition key |
| `garmin_reference` | `static`, `static_goals`, `weekly`, `per_profile`, `per_device` | Unpartitioned | The UTC fetch date |

A daily partition key is a calendar date. The asset passes it to Garmin as the request's
`cdate`, or as both `start` and `end`, and Garmin interprets that as the user's **local**
calendar day. Partitions open on UTC day boundaries. The 07:00 UTC schedule runs after the
user's local day has also ended. Further local-day semantics (for example, a sleep night is
keyed by local wake date) are applied in silver, never in this package.

The `activities` asset captures the day's activity list (`get_activities_by_date(d, d)`), then
fans out per activity: one capture per `PER_ACTIVITY` endpoint (`activity_summary`,
`activity_details`, splits, zones, weather, gear…) and one raw download per
`PER_ACTIVITY_DOWNLOAD` format (the FIT `.zip`). Each lands in that day's `dt` folder for its
own collection. No two collections share a folder.

## Capture cadence: daily re-capture window

Two schedules run at 07:00 UTC:

- **`garmin_daily`** emits one run per partition in the trailing `LOOKBACK_DAYS` (default 7)
  **completed** daily partitions. The current, still-open day is never captured. Each tick
  re-captures all of those partitions, whether or not they were captured before. So every
  completed day is captured on about `LOOKBACK_DAYS` consecutive days, and each capture can
  pick up data Garmin synced or restated since the previous one.
- **`garmin_reference`** emits one run per day that refreshes all reference collections.

Each `garmin_daily` run's `run_key` is the partition key **plus the tick**
(`<partition>-<YYYYMMDDTHH>`), matching the Whoop and USCRN schedules. Dagster dedupes a
schedule's run keys only within one tick (so a retried tick doesn't double-launch). A
tick-scoped key states in code that re-emitting a partition on a later tick is intended. It
also stops a future move to a sensor, where run keys are deduped across all evaluations, from
silently turning re-capture into capture-once.

`LOOKBACK_DAYS` is the one knob for the window. A larger value catches later restatements at
the cost of more API calls per tick. Storage cost barely grows, because unchanged
re-captures are skipped (next section).

## Bronze writes: skip-identical capture

Garmin data is written in two capture grades, recorded in each sidecar's `capture_mode`:
`reserialized` (the library's parsed JSON, re-serialized deterministically) and `raw`
(binary activity downloads, byte-for-byte). Profile and settings collections are run through
the secret screen before capture, and dropped key paths are recorded in `redacted_fields`.

**What reaches the writer.** A `None` return from Garmin is never captured, for any endpoint.
An empty list or dict is captured as a faithful "asked, no data" record, unless the endpoint
is flagged `skip_if_empty`, in which case nothing is written. Both rules apply before the
writer runs.

**The skip-identical check** (`dedupe=True`) skips writing a payload whose sha256 equals the
sha256 recorded in the newest sidecar in that `(collection, dt)` folder. Garmin applies it
only where it is sound on both of these counts:

1. **One request per folder per run.** The check compares against only one file. When several
   distinct requests write into one folder in a run (one per activity, device or goal status),
   two different requests can return identical bytes, most often an empty `[]` or `{}`.
   Skipping the second would drop the faithful record that the request was asked and answered,
   along with its `request_params` provenance.
2. **The endpoint writes on every run.** Freshness checks rely on a new sidecar arriving every
   day (see *Interaction with bronze checks*). An endpoint that writes nothing on no-data days
   (any `skip_if_empty` endpoint, or one whose no-data answer is `None`, such as `hrv`) can go
   days without writing a new partition's first capture. Without that, only its re-captures of
   older days keep freshness alive. Those endpoints therefore keep every capture. The catalog's
   `skip_if_empty` / `skip_if_none` flags are how an endpoint declares this. Every unflagged
   daily endpoint writes a capture for every day in bronze. An unflagged endpoint that starts
   returning `None` on no-data days must get `skip_if_none` set.

| Capture path | Skip-identical |
|---|---|
| Single-call `daily` / `range` endpoints that always return a value, and the `activities` list | **On** |
| `static` / `weekly` / `per_profile` reference endpoints that always return a value | **On** (only matters when a reference run repeats on the same UTC day, since each daily run writes a new fetch-date folder) |
| Endpoints flagged `skip_if_empty` or `skip_if_none`, whatever their kind | Off: keep every capture |
| Per-activity fan-out (`PER_ACTIVITY` endpoints and activity downloads) | Off: keep every capture |
| `per_device` and `per_device_range` endpoints | Off: keep every capture |
| `static_goals` (`goals`, one request per status) | Off: keep every capture |

The flag is decided **at each capture call site** from the capture path and the endpoint's
flags. It is never derived from `Endpoint.kind` alone: the per-activity fan-out builds its
endpoints with kind `static`, which would wrongly turn the check on.

A skipped capture never loses data, because the newest capture in the folder already holds
exactly those bytes. If the newest sidecar is missing a `sha256`, unreadable or absent, the
check gives way and the payload is written.

## Contract with silver

A `(collection, dt)` folder may hold **several captures**. Each one is a complete snapshot of
what Garmin returned at its `fetched_at`. Silver must choose among them by recency: the latest
capture per key wins (`dedup_latest_sql` ordered by the fetch time in the filename). Silver
must never assume one file per day.

Skip-identical leaves silver's output unchanged **as long as each silver key is fed from a
single `(collection, dt)` folder**. A skipped payload is byte-identical to the folder's newest
capture, which already wins within that folder. The Garmin-fed silver tables meet this today:
sleep by `calendarDate`, daily by `user_summary` day, body by weigh-in `samplePk`, fitness by
`dt`, workouts by `activityId` from the day's list, and splits from a keep-every-capture
collection. A new silver table keyed on something that spans several day folders (a week, for
example) must not rely on this guarantee.

## Interaction with bronze checks

Checks are generated from the catalog (`dagster/checks.py`).

- **Freshness** (every collection) reads the newest sidecar's `fetched_at` across recent
  partitions, with a tolerance of 26 h cadence + 6 h grace. Skip-identical never starves it.
  Where the check is on, the endpoint returns a value every run, so each daily tick's newly
  completed partition gets a first capture into an empty folder, which always writes. Each
  reference run writes into a new fetch-date folder. Endpoints that can skip a day keep every
  capture, so their re-captures of older days keep producing sidecars, as they always have.
- **Completeness** reads only which `dt=` folders exist. Unaffected.
- **Schema drift** reads the richest of the newest payloads per folder. Unaffected.
- **Content health** samples payloads across folders. Fewer identical copies only make the
  sample more varied.

## Failure and recovery

- **Missed ticks.** A tick-scoped `run_key` launches a fresh run on every tick. Any partition
  missed during an outage shorter than `LOOKBACK_DAYS` is captured by the next tick. After a
  longer outage, the days that fell out of the window stay missing until a manual partition
  backfill of `garmin_daily_job`. The completeness check (2-day cadence) warns about the gap.
- **Overlapping runs.** A backfill or a retried run may capture the same partition while a
  scheduled run does. The two captures may both pass the skip-identical check and write. The
  only cost is an extra identical file. Concurrency against the Garmin API is bounded by the
  `garmin_api` pool limit, which is set on the host instance.
- **Capture failures.** The core writer never raises: a failed write is logged and the run
  continues. Real exceptions from Garmin (auth, 429, transport) propagate and fail the asset
  visibly.

## Auth, catalog and selection

*(not yet specified in detail. Summary of current design.)* Auth is delegated to
`garminconnect`, with its own self-healing token store at `GARMINTOKENS` (never under
`BRONZE_ROOT`) and a one-time interactive MFA bootstrap. The catalog is an allowlist:
mutating and auth methods are never callable, and a drift detector surfaces new readable
library methods for review. `FETCH_SELECTION` / `FETCH_EXCLUDE` (exclusion wins) turn
endpoints off at runtime. A deselected asset materializes as a no-op and has no checks. All
ops in a run share one logged-in client via the in-process executor and the `garmin_api`
concurrency pool.

## Decisions & Alternatives

| Decision | Chosen | Alternatives Considered | Rationale |
|---|---|---|---|
| How often a completed day is captured | Re-capture each of the trailing `LOOKBACK_DAYS` completed days on every daily tick | (a) Capture each partition exactly once (skip already-materialized partitions, or a sensor with partition-key run keys); (b) a shorter 2–3 day window | Garmin restates and back-fills days after the fact. Bronze shows most important collections changing on the second capture and some through the seventh. Capture-once freezes each day at its first, often sparse, pull. That degrades silver and makes the sparse newest-day stub permanent. A short window drops the day-4-to-7 corrections. *Completeness over economy*. |
| Storage control for re-captures | Core skip-identical (`dedupe=True`) | Keep every capture (`dedupe=False`); shrink the window | About 71% of re-captures are byte-identical to the previous capture. Skipping them never loses data and reuses the shared core mechanism Whoop and USCRN already use. |
| Which captures may be skipped | Single-request-per-folder paths whose endpoint returns a value every run | Skip-identical everywhere | Two soundness conditions. Fan-out folders would drop identical answers to different requests. Endpoints that skip no-data days would starve freshness and page falsely. The excluded paths are cheap except per-activity (next row). |
| Per-activity fan-out storage | Keep every capture | Teach the core writer to compare against the newest capture of the **same request** (matching `request_params`) | The per-activity collections are about 38% of Garmin bronze bytes (FIT downloads dominate), so they keep growing about 7× per activity. Per-request comparison is the right fix but changes the shared writer, which every source uses. Keeping every capture is complete and safe now. The core change is deferred. |
| Schedule `run_key` | `<partition>-<YYYYMMDDTHH>` (tick-scoped) | `run_key = partition_key` | Matches Whoop and USCRN, and states in code that re-emitting a partition each tick is intended. A partition-only key reads as capture-once and becomes capture-once under a sensor. |
| Duplicate captures already in bronze | Leave them | Delete identical older captures | Bronze is immutable and append-only. Silver already picks the latest capture, so the extra files are harmless. |
| Partition range | Completed days only (`end_offset=0`), captured at 07:00 UTC | Include the in-progress day | A partial "today" would be captured repeatedly while still incomplete. The trailing window picks the day up once it has closed, and 07:00 UTC is after the user's local day ends. |
| Run shape | One job, in-process executor, one shared client per run | Multiprocess executor with per-op logins | One login per run avoids repeated Garmin SSO logins and the MFA and rate-limit pressure they cause. |

## Open Questions & Future Decisions

### Deferred

1. **Window length.** Some collections (`activities_fordate`, `heart_rates`,
   `intensity_minutes`) still change on the sixth or seventh capture. It's unknown whether
   that is genuine restatement (which would argue for a larger `LOOKBACK_DAYS`) or a volatile
   field that changes on every request. Decide after inspecting the late diffs.
2. **Collections that change on every pull.** `lifestyle_logging` and `device_settings`
   differ on nearly every capture. For `device_settings`, part of the reason is that several
   devices interleave in one folder. If the remaining difference is a volatile field (a request
   timestamp, for example), these keep growing about 7× per day. A Garmin-side normalization
   would have to be weighed against bronze's byte-faithful rule.
3. **Per-request skip-identical in core.** An opt-in match key on `capture_bronze` (compare
   against the newest capture with the same `request_params`) would let the fan-out paths skip
   identical re-captures too. This is a change to the shared writer, owned outside this
   package.
4. **Orphan payloads in core.** The writer saves the payload before its sidecar. If the sidecar
   write fails, the payload is an orphan: silver reads it, but the skip-identical check
   (sidecar-based) doesn't see it. A later capture identical to the older, sidecar-backed
   capture would then be skipped while silver keeps the orphan. This needs a failed sidecar
   write and is owned by the core writer.
5. **Written / skipped / failed counts.** The core writer returns `None` both for a skipped and
   for a failed capture, so a run that wrote nothing looks like a healthy run. Per-run counts in
   materialization metadata would separate the two. This needs the writer to distinguish the
   outcomes.

## References

- `docs/high-level-design.md`: tenets and the bronze re-capture decision
- `docs/BRONZE.md`: bronze layout, sidecar and writer invariants
- `docs/adr/0004-garmin-port.md`: the original Garmin port decision record
- `packages/core/src/grecohome_core/bronze/capture.py`: `capture_bronze` and skip-identical
- `packages/core/src/grecohome_core/silver/transform.py`: `dedup_latest_sql`
