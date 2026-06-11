# Specification: Bronze Validation as Dagster Asset Checks

**Target repo:** `grecohome-dagster` (uv monorepo; `packages/core` = `grecohome-core`, plus
`whoop`, `garmin`, `lingo`, `soil` subject packages).
**Goal:** Bring bronze-layer data quality/observability *into Dagster* as `@asset_check`s, so the
existing standalone validation logic (freshness, completeness, schema drift, integrity/content)
runs with materializations, surfaces in the Dagster UI, and fails in the same place runs are
watched. **No change to capture logic.** Checks are read-only over `BRONZE_ROOT`.

**Dagster version:** pinned `dagster==1.13.8` / `dagster-*==0.29.8`. Use only asset-check APIs
available in that version. Do **not** introduce APIs from later releases without verifying.

---

## 0. Scope and non-goals

In scope:
- A reusable check framework in `grecohome-core` (the checks are generic; subjects wire them).
- Per-subject wiring of checks onto each subject's existing bronze assets.
- Four check families: **fetch-freshness**, **event/partition completeness**, **schema drift**,
  **content health** (empty/error payload share). Definitions in §3.
- Integration so checks appear in the UI and (where appropriate) block/█warn without breaking capture.

Explicitly NOT in scope:
- Any modification to `capture_bronze`, the sidecar contract, schedules, sensors, or the
  capture path. Checks never write to `BRONZE_ROOT`.
- Silver/gold. This finishes bronze only.
- Replacing the existing standalone `verify_bronze.py` / `sweep_streams.py` immediately — those
  may be retired *after* the in-Dagster checks are confirmed equivalent, but that is a follow-up,
  not part of this work.
- Alerting/notification wiring (Slack/email). Surfacing in the UI + check status is the deliverable.

---

## 1. Design principles

1. **Checks read bronze; they never write it.** All checks query files under `BRONZE_ROOT`
   read-only (DuckDB over the JSON/CSV/txt, or direct file/sidecar reads). They must never create,
   modify, or delete anything under `BRONZE_ROOT`.
2. **Sidecars must be excluded from every read.** `*.meta.json` files sit beside payloads and
   share the `.json` suffix. Every payload glob must exclude them, and — critically — for any
   operation that infers a *schema/column union* (DuckDB `read_json`/`read_csv_auto` over a glob),
   a row-level `WHERE filename NOT LIKE '%.meta.json'` is **insufficient** (the column union is
   computed across all matched files before the row filter). For schema inference, read a single
   known payload file by exact path (glob the file list, filter out `.meta.json` in Python, read
   one). This is a known, tested gotcha — bake it into the shared helper so no subject re-hits it.
3. **Generic logic in core, specifics in subjects.** The check *implementations* live in
   `grecohome-core` as parameterized builders. Each subject supplies only its specifics: which
   collections, each collection's event-date expression (or None), and cadence tolerances. Mirrors
   the existing core-vs-subject split used for capture.
4. **Severity is deliberate per check.** Use Dagster `AssetCheckSeverity`. Freshness and schema
   drift default to **ERROR** (these mean the pipeline or source is broken). Completeness gaps and
   content-empty default to **WARN** (often legitimate — device not worn, intermittent CGM,
   hardware-unsupported endpoints). See §3 for per-check severity and §4 for per-source overrides.
5. **Checks must be cheap and bounded.** A check runs over recent data by default (e.g. trailing
   N partitions), not the entire history every time, so checks stay fast as bronze grows. Full-history
   variants may exist but must be opt-in.
6. **Never raise out of a check.** A check that errors internally should return a failed
   `AssetCheckResult` (or let Dagster mark it failed), not throw in a way that breaks the run.
   Mirror the capture path's non-fatal philosophy.

---

## 2. Where things live

```
packages/core/grecohome_core/
  checks/
    __init__.py
    bronze_reads.py     # shared read helpers: payload globbing, sidecar-safe reads,
                        # DuckDB connection, single-payload schema read, event-date distinct query
    freshness.py        # build_fetch_freshness_check(...)
    completeness.py     # build_event_completeness_check(...)
    schema_drift.py     # build_schema_drift_check(...)  + baseline storage helpers
    content_health.py   # build_content_health_check(...)
    config.py           # CollectionCheckConfig dataclass (the per-collection spec)
```

Each subject package wires checks next to where it defines assets, e.g.:

```
packages/whoop/grecohome_whoop/dagster/
  definitions.py        # existing: assets, jobs, schedules
  checks.py             # NEW: builds the check list from per-collection configs, exports them
```

Subjects register their checks in their `Definitions(...)` via the `asset_checks=[...]` argument
(or attach via `check_specs` on assets where that pattern fits the existing asset style — choose
whichever matches how assets are currently defined; see §5).

---

## 3. The four check families

All builders take a `CollectionCheckConfig` (see §3.5) and return a Dagster asset check bound to
the subject's bronze asset for that collection.

### 3.1 Fetch-freshness  (severity: ERROR)

**Question:** "Have we captured this collection recently enough?"
**How:** read the newest `fetched_at` across the collection's sidecars (`*.meta.json`), compute
hours since. Compare to `cadence_hours + grace_hours`.
**Why sidecars:** every payload has a sidecar with `fetched_at` regardless of payload shape, so
this is the one universal signal that works for all collections/sources uniformly.
**Pass:** newest fetch within tolerance. **Fail (ERROR):** stale beyond tolerance, or no sidecars
found for a configured collection (a configured collection with zero captures is alarming).
**Metadata to attach:** `last_fetch`, `hours_since`, `tolerance_hours`, `sidecar_count`.

### 3.2 Event / partition completeness  (severity: WARN)

**Question:** "Are there gaps in the event timeline beyond this collection's expected cadence?"
**How:** distinct event dates from payloads (using the collection's `event_date_sql`), ordered;
flag consecutive gaps `> cadence_days`. For collections whose `dt` *is* the event date
(partition-dated sources), the event date may come from the partition rather than payload contents
— see §3.5 `event_date_source`.
**Important nuance per BRONZE.md:** `dt` is the *partition* date for Whoop daily collections,
Garmin daily, and USCRN; but `dt` is the *fetch* date for Lingo glucose and Whoop snapshots
(`profile`, `body_measurement`). So completeness must use the **true event date**:
- Whoop sleep/recovery/workout/cycle: event date from payload (`start`/`created_at`) — these
  rescore, so dedup keeps one logical record; completeness counts distinct event days.
- Garmin daily collections: event date = partition `dt` (capture-once, immutable).
- Lingo glucose: event date is **inside the CSV** (the reading timestamp), NOT `dt` (which is
  fetch date). Parse the timestamp column. Expect heavy intermittency (CGM worn in bursts) →
  WARN only, wide tolerance.
- USCRN hourly: event date = partition `dt` (= the UTC date column).
**Pass:** no gaps beyond `cadence_days`. **Warn:** gaps found — surface them, do not fail; gaps are
frequently legitimate (device not worn, sensor off, hardware unsupported).
**Metadata:** `event_days`, `earliest`, `latest`, `gaps_over_cadence`, `biggest_gap_days`.
**Collections with `event_date_source=None`** (current-only snapshots like Whoop `profile`):
skip completeness entirely — only freshness/schema apply.

### 3.3 Schema drift  (severity: ERROR)

**Question:** "Has the payload's top-level shape changed from the recorded baseline?"
**How:** compute a stable signature of the payload's top-level keys/columns from **one** payload
file (read by exact path — see §1.2). For `records[]`-wrapped JSON (Whoop), signature = sorted
top-level keys of the unnested record. For flat JSON (Garmin `dailySleepDTO`-style), sorted
top-level keys of the object (drop hive `dt`). For CSV (Lingo, USCRN), sorted column names (drop
`dt`). Compare to the stored baseline; if no baseline, record current as baseline (status
`baseline_set`, pass). If changed, fail ERROR with before/after.
**Baseline storage:** a JSON file **outside `BRONZE_ROOT`** (e.g. under a `BRONZE_MONITOR_DIR`
mount, or a dedicated metadata dir). Must be writable by the check; must never be written under
`BRONZE_ROOT`. Document the mount in `docs/DEPLOYMENT.md` as part of this work.
**Metadata:** `status` (`ok`/`drift`/`baseline_set`), `was`, `now`.

### 3.4 Content health  (severity: WARN)

**Question:** "Are recent payloads carrying real data, or are they empty/error envelopes?"
**How:** sample recent payloads; classify each (DATA / EMPTY_LIST / EMPTY_OBJECT / EMPTY_WRAPPER
i.e. `{"records":[]}` / ERROR_LIKE i.e. error-keyed with no data / HTTP_ERROR from sidecar
`http_status` non-2xx / CSV_DATA). This is the `sweep_streams.py` classification, run continuously.
**Pass:** payloads are DATA (or CSV_DATA). **Warn:** meaningful share empty/error — but note many
collections are *legitimately* always-empty on this hardware (Garmin `hrv`, `training_readiness`,
etc. per BRONZE.md). Those should be configured as **expected-empty** (see §4) so they don't warn.
**Metadata:** per-class counts over the sampled window.

### 3.5 `CollectionCheckConfig` (the per-collection spec)

A dataclass in `core/checks/config.py`. Each subject builds a list of these. Fields:

```
@dataclass(frozen=True)
class CollectionCheckConfig:
    source: str                     # "whoop"
    collection: str                 # "sleep"
    asset_key: AssetKey             # the existing bronze asset this check attaches to
    reader: Literal["json","csv","txt"]
    unnest_records: bool            # True if payload is {"records":[...]}
    event_date_source: Literal["payload","partition","none"]
    event_date_sql: str | None      # DuckDB expr yielding a DATE when source=="payload"
                                    #   (over unnested record alias `r` for json, or a column for csv)
    cadence_hours: float            # freshness tolerance (before grace)
    cadence_days: int               # completeness gap tolerance
    expected_empty: bool = False    # True -> content-health passes on empty (hardware-unsupported)
    enabled_checks: frozenset[str] = frozenset({"freshness","completeness","schema","content"})
    recent_partitions: int = 14     # bound checks to trailing N partitions by default
```

The builders read this and produce the appropriate `@asset_check`. Subjects only write configs +
call builders; they do not reimplement check logic.

---

## 4. Per-source wiring (use the real asset names from each subject's definitions)

> Claude Code: pull the actual `AssetKey`s from each subject's `definitions.py`. The asset/
> collection names below come from the live Dagster instance and BRONZE.md; confirm against code.

**Whoop** (`whoop_ingest`): collections `sleep`, `recovery`, `workout`, `cycle` (daily-partitioned,
event date from payload; rescored so dedup-on), plus `profile`, `body_measurement` (current-only
snapshots → `event_date_source="none"`, completeness disabled, fetch-date folders).
- sleep/recovery/cycle: `cadence_hours≈26`, `cadence_days=2`.
- workout: `cadence_days` wide (e.g. 10–14) — workouts are intermittent; the user had a real multi-
  week gap (surgery). WARN only.
- All Whoop: `unnest_records=True`, `reader="json"`.

**Garmin** (`garmin_ingest`): many daily collections (`sleep`, `stress`, `hrv`, `respiration`,
`steps_intraday`, `resting_heart_rate`, `spo2`, `training_status`, `max_metrics`, etc.), capture-
once/immutable (`event_date_source="partition"`), plus reference/snapshot collections.
- Mark known hardware-unsupported / legitimately-empty collections `expected_empty=True`
  (per BRONZE.md examples: `hrv`, `training_readiness`; also the empties found in prior analysis:
  `activities` list endpoint, `max_metrics` mostly-empty, `body_battery_events`, `activity_gear`,
  `goals`, `running_tolerance`). Confirm the current set from a content sweep before finalizing.
- `unnest_records=False` for the flat DTO-style payloads; `reader="json"`.
- Keep checks lightweight given the ~65 collections — bound to `recent_partitions` and prefer the
  universal freshness check across all, with completeness/schema only on the analytically-important
  ones (sleep, stress, hrv, training_status, the daily time-series).

**Lingo** (`lingo_ingest`): one collection `glucose`, `reader="csv"`, `event_date_source="payload"`,
`event_date_sql` parses the reading-timestamp column to a DATE (the CSV header is
`Time of Glucose Reading [T=(local time) +/- (time zone offset)]`). `dt` is fetch date, NOT event
date — completeness MUST use the in-CSV timestamp. Expect heavy gaps (intermittent wear) → WARN,
`cadence_days` wide. Dedup-on means many overlapping rows across files → completeness must operate
on DISTINCT event dates.

**Soil/USCRN** (`soil_ingest`): one collection `hourly`, `reader="txt"`,
`event_date_source="partition"` (dt = UTC date), cadence daily. Payload is raw text rows; schema-
drift on a txt payload may be column-count/format based rather than JSON keys — define a simple
signature (e.g. field count of first row) or mark schema check disabled if not meaningful.

---

## 5. Dagster integration details (1.13.8)

- Use `@dagster.asset_check(asset=<bronze asset key>, name=..., blocking=False)` for the builder
  output. Default `blocking=False` so a failed check surfaces but does not abort the materialization
  (capture must remain robust). Reserve `blocking=True` only if/where a check should gate downstream
  silver later — not now.
- Return `dagster.AssetCheckResult(passed=bool, severity=AssetCheckSeverity.WARN|ERROR,
  metadata={...}, description=...)`.
- Register checks in each subject's `Definitions(..., asset_checks=[...])`. Verify the subject
  currently constructs a `Definitions` object and add to it; do not create a parallel definitions
  object.
- **Freshness:** the instance already runs the `FRESHNESS_DAEMON`. Where it cleanly fits, prefer
  Dagster's native freshness mechanism for the freshness signal instead of a hand-rolled
  `@asset_check`, to avoid reimplementing "hours since last update." Evaluate
  `build_fetch_freshness_check` vs a native freshness policy on each bronze asset and choose one
  consistently; document the choice. (If the native freshness API surface in 1.13.8 is awkward for
  these partitioned/snapshot assets, fall back to the `@asset_check` implementation — but check
  first, since the daemon is already running and it's the lower-maintenance path.)
- Checks should be invocable two ways: automatically with each asset materialization, AND on a
  schedule independent of materialization (so freshness/staleness is detected even if an asset
  *stops* materializing). Provide a small job/schedule in core that executes all bronze checks on a
  cadence (e.g. hourly), in addition to per-materialization execution. A stopped asset is exactly
  the failure we most need to catch, and per-materialization checks can't catch "it stopped."
- **Concurrency:** check runs read bronze locally (no source API calls), so they must NOT use the
  `garmin_api` / `whoop_api` concurrency pools. Ensure check jobs are unpooled (or on a separate
  `validation` pool) so they never contend with — or get starved by — ingestion runs. (This repo
  just had an incident where a single API-pool slot wedged a subject; keep validation off those
  pools entirely.)

---

## 6. Testing & acceptance

Per the repo's existing `pytest` setup (`uv run pytest`):

- **Unit tests in core** for each builder using a tiny synthetic bronze tree (a few payloads +
  sidecars, including: a stale one, an interior gap, a drifted schema, an empty/error payload, and
  a `.meta.json` beside each payload). Assert each check returns the expected pass/fail + severity.
  The synthetic tree MUST include sidecars so the sidecar-exclusion logic is exercised (this is the
  bug class most likely to regress).
- **Sidecar-contamination regression test:** explicitly assert the schema-drift signature for a
  flat-JSON collection does NOT contain sidecar field names (`sha256`, `fetched_at`,
  `stored_encoding`, etc.). This guards §1.2.
- **Lingo event-date test:** assert glucose completeness uses the in-CSV timestamp, not `dt`
  (construct two fetch-date folders whose CSVs contain overlapping/older event dates and confirm
  the event span reflects CSV contents).
- **Expected-empty test:** a collection marked `expected_empty=True` passes content-health on an
  empty payload; an un-marked one warns.
- `uv run ruff check` clean; `uv run pytest` green.

Acceptance:
- [ ] Checks appear in the Dagster UI attached to the correct bronze assets, per subject.
- [ ] A deliberately stale collection shows a failed freshness check (ERROR).
- [ ] A known-empty Garmin collection marked `expected_empty` does NOT warn; an unexpected empty does.
- [ ] Schema baselines are written outside `BRONZE_ROOT`; drift is detected on a changed signature.
- [ ] A standalone validation job/schedule runs all bronze checks hourly, off the API concurrency
      pools, and is visible in the UI.
- [ ] No capture-path file changed; checks never write under `BRONZE_ROOT`.
- [ ] Port the existing standalone logic faithfully: the four families match
      `verify_bronze.py` + `sweep_streams.py` behavior (freshness, completeness w/ gap surfacing,
      schema drift w/ baseline, content classification).

---

## 7. Suggested commit/PR sequence

1. `core/checks/bronze_reads.py` + `config.py` + unit-test scaffold (the sidecar-safe read helpers
   are the foundation; get them right and tested first).
2. `freshness.py` + `schema_drift.py` (the two ERROR checks) with tests.
3. `completeness.py` + `content_health.py` (the two WARN checks) with tests, including the Lingo
   event-date and expected-empty cases.
4. Wire **one** subject end-to-end (Whoop) — configs, `checks.py`, register in `Definitions`,
   confirm in UI. Validate the pattern before fanning out.
5. Wire remaining subjects (Garmin, Lingo, Soil), using the validated Whoop pattern.
6. Add the core "run all bronze checks" job + hourly schedule, off the API pools.
7. Update `docs/` (a new `docs/VALIDATION.md` describing the check families, severities, the
   baseline mount, and the expected-empty list; note the standalone scripts are superseded).

---

## 8. Notes / gotchas to carry over (hard-won)

- **Sidecar exclusion is the #1 recurring bug.** `*.meta.json` ends in `.json`; row filters don't
  fix schema inference; read one exact payload file for signatures. Centralize in `bronze_reads.py`.
- **`dt` ≠ event date for Lingo glucose and Whoop snapshots.** Completeness must use the true event
  date (in-CSV timestamp / payload field), not the partition folder.
- **Gaps are usually legitimate.** Completeness and content-empty are WARN, not ERROR. Don't let
  them cry wolf or they'll be ignored.
- **Validation runs must stay off `garmin_api`/`whoop_api` pools** — they make no source calls and
  must not contend with ingestion or be wedged by it.
- **Baselines and any check state live OUTSIDE `BRONZE_ROOT`** — bronze stays immutable raw capture
  only; checks never write there.
- **Pinned Dagster 1.13.8** — verify asset-check / freshness APIs against that version; the daemon
  set already includes `FRESHNESS_DAEMON` and `MonitoringDaemon`.