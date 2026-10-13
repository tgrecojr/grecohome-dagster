---
parent: core
prefix: CORE-CHECKS
---

# Bronze checks

## Context and Design Philosophy

Bronze data quality runs inside Dagster as `@asset_check`s attached to each subject's bronze
assets. Failures show up in the UI next to materializations, page through Slack when they
matter, and catch the one failure a per-run check can't: **an asset that stops capturing.**

Core provides parameterized **builders**; each subject supplies a list of
`CollectionCheckConfig`s and calls them. A subject never reimplements check logic.

**Principles:**

1. **Read-only over bronze.** Checks never create, modify or delete anything under
   `BRONZE_ROOT`. The only state they write (schema baselines) lives under
   `BRONZE_MONITOR_DIR`, outside bronze.
2. **Sidecars are excluded from every payload read.** `*.meta.json` shares the `.json` suffix.
   Payload listing filters it out, and schema signatures read **one exact payload file**
   (never a glob), because a glob-based schema union would mix sidecar fields (`sha256`,
   `fetched_at`, …) into the inferred schema. This is the historically recurring bug, so it's
   centralized in `grecohome_core.checks.bronze_reads`, and a regression test asserts that a
   flat-JSON signature never contains sidecar field names.
3. **Severity is deliberate.** A broken pipeline or source contract is **ERROR** (freshness,
   schema drift, byte corruption). Gaps and empty payloads are frequently legitimate (device not
   worn, intermittent sensors, hardware without the feature), so they're **WARN**. A check that
   cries wolf gets ignored.
4. **Bounded.** Each check looks at the collection's `recent_partitions` most recent **existing**
   `dt=` folders (default 14, sorted by name; 0 or less means all of them), so checks stay fast
   as bronze grows. For a sparse collection those 14 folders can span weeks.
5. **Never raise.** An internal error returns a failed result; it never breaks the run.
6. **Non-blocking.** Every check is `blocking=False`: a failure is reported but never aborts the
   materialization.

## `CollectionCheckConfig`

| Field | Meaning |
|---|---|
| `source`, `collection`, `asset_key` | The bronze path segments and the asset the checks attach to |
| `reader` | `json`, `csv` or `txt`: the stored payload form |
| `unnest_records` | The JSON payload is a `{"records": [...]}` wrapper; signatures and event dates look at a record |
| `event_date_source` | `payload` (with `event_date_field`, a JSON key with dotted paths allowed, or a CSV column), `partition` (the `dt` folder is the event date), or `none` (a current-only snapshot: completeness is skipped) |
| `cadence_hours` (26), `grace_hours` (6) | Freshness tolerance |
| `cadence_days` (2) | Completeness gap tolerance |
| `expected_empty` (false) | The collection is legitimately empty on this hardware |
| `enabled_checks` (all four) | Which families to build: `freshness`, `completeness`, `schema`, `content` |
| `recent_partitions` (14) | How many trailing folders each check reads |

`payload` requires `event_date_field`, and unknown family names are rejected, both when the
config is built. Check names are `<source>_<collection>_<family>` (families: `freshness`,
`completeness`, `schema_drift`, `content_health`).

Subjects build checks with `build_collection_checks(configs, bronze_root=..., monitor_dir=...)`,
which calls `build_checks_for` per config. That builds each family in `enabled_checks`, except
completeness when `event_date_source` is `none` (no check is built). Silver and gold reuse the
same job and schedule builders for their own checks.

## The four families

### Freshness (ERROR)

*Are we still capturing this?* It reads the newest `fetched_at` from the sidecar **filenames**
across the trailing folders (no file opens) and fails when it's older than
`cadence_hours + grace_hours`. It also fails when a configured collection has **no captures**,
unless `expected_empty`. Sidecars whose names carry no parseable time are counted but can't
supply a time; if none parse, the collection is treated as having no captures.

Sidecars are the one signal every capture carries, whatever the payload's shape. A native
Dagster freshness policy keys off materialization time instead. Trailing-window assets
re-materialize constantly whether or not data arrives, so that clock wouldn't reflect actual
captures.

Freshness measures the last **written** capture. With skip-identical on, it's reliable only
when the collection writes a new file regularly (a new partition or folder each day, or content
that changes). Each subject decides per collection whether that holds.

### Completeness (WARN)

*Are there gaps in the event timeline?* It collects the distinct **event dates** over the
trailing folders and warns when two consecutive event dates are **more than `cadence_days`
apart**. With `cadence_days=2`, one missing day passes and two missing days warn. The
metadata reports the missing days (difference − 1): `event_days`, `earliest`, `latest`,
`gaps_over_cadence`, `biggest_gap_days`.

| `event_date_source` | Event date from |
|---|---|
| `payload` | The record field (JSON) or column (CSV) named by `event_date_field`. Parsing is best-effort: ISO dates and datetimes (the first 10 characters), then `%m/%d/%Y`, `%m-%d-%Y` and `%Y/%m/%d`, and numbers as epoch seconds (UTC). Values that won't parse are ignored |
| `partition` | The `dt=` folder |
| `none` | No completeness check is built for the collection |

Fewer than two event dates passes ("too few to assess"). It is WARN only: it surfaces gaps, and
never fails the run over them.

### Schema drift (ERROR)

*Has the payload's top-level shape changed?* A signature is computed from the **richest** recent
payload: the newest payload of each trailing folder, keeping the one with the most top-level
tokens (ties go to the newest). Only each folder's newest payload is considered: if it's empty
or unreadable, that folder contributes nothing, with no fallback to an older file in it.

- **Signature:** sorted top-level keys of a JSON payload or, with `unnest_records`, of a record.
  For CSV, the column names minus a `dt` column; for text, the field count of the first row.
- **Empty JSON and CSV payloads are ignored.** They have no schema, so they can neither set nor
  contradict a baseline. An empty text file signs as `fields=0`, so it does count.
- **Why the richest payload:** taking only the newest would let a thin, not-yet-synced "stub"
  payload define the schema and drift falsely.

The signature is compared with a baseline at
`BRONZE_MONITOR_DIR/schema_baselines/<source>/<collection>.json`.

| Situation | Result |
|---|---|
| No non-empty payload in the window | Pass, `no_payload` |
| `BRONZE_MONITOR_DIR` unset | Pass, `disabled` (it never falls back to writing under bronze) |
| Baseline path would resolve inside `BRONZE_ROOT` | **Fail, ERROR**, `error` (the write is refused; this pages) |
| No baseline, an empty one, or an unreadable one | Record the current signature, pass, `baseline_set` (see *Open Questions* for the unreadable case) |
| Signature equals the baseline | Pass, `ok` |
| Signature differs | **Fail, ERROR**, `drift`, with `was`, `now`, `added` and `removed` |
| Internal error | **Fail, ERROR**, `error` |

To accept a real source change, the operator removes the baseline file and the next run
re-records it.

### Content health (WARN; ERROR on corruption)

*Are recent payloads carrying real data, and intact?* It samples **5** payloads (the builder's
default) spread across the trailing folders, and does two things with each.

- **Classifies it** by file extension (not by `reader`):
  - `HTTP_ERROR` (sidecar status outside 2xx), checked first;
  - a non-`.json` file is `TXT_DATA` (`.txt`), `CSV_DATA` (any other extension, including
    `.bin` and `.zip`), or `EMPTY_FILE`;
  - a `.json` file is `UNPARSEABLE`, `EMPTY_FILE` (JSON `null`) or, once parsed, `DATA`,
    `EMPTY_LIST`, `EMPTY_OBJECT`, `EMPTY_WRAPPER` (an empty data array with nothing but
    pagination keys beside it), or `ERROR_LIKE` (a real error key and no data array).
- **Verifies its integrity:**
  - the on-disk sha256 and byte size against the sidecar;
  - a missing or unreadable sidecar, or one with no `sha256`;
  - a payload that can't be read;
  - a **zero-byte** payload;
  - a JSON payload that doesn't parse (checked when `stored_encoding` is `identity`).

| Outcome | Result |
|---|---|
| Any integrity issue | **Fail, ERROR** (corruption outranks emptiness). A zero-byte payload is an integrity issue, so it's ERROR even for an `expected_empty` collection |
| Any empty, error-like or unparseable sample | Fail, WARN. `expected_empty` collections pass on empties; `HTTP_ERROR`, `ERROR_LIKE` and `UNPARSEABLE` still warn |
| Internal error | Fail, WARN |
| No payloads to sample | Pass (freshness covers absence) |
| Otherwise | Pass |

## Alerting

Dagster OSS has no push alerting: a failed check is a framework event in Postgres, not a log
line. The `alerting_check` decorator closes that gap. When a wrapped check returns a **failing
ERROR** result, it logs one structured `asset_check_failed` line at ERROR, carrying only `check`
(the name) and `asset`, through the shared logger, which ships to Loki. A Grafana Loki rule on
that line posts to Slack, so one rule covers every layer's page-worthy checks. WARN failures
aren't emitted; they stay in the UI and don't page.

- An error while logging is swallowed and never breaks the check. An exception raised by the
  check itself isn't caught by the decorator.
- Freshness, schema drift and content health are wrapped. Completeness, which is WARN-only, is
  not.
- Internal errors in freshness and schema drift return ERROR results, so they page. Internal
  errors in completeness and content health return WARN, so they don't.
- Bespoke checks (token health, for example) use the decorator too.

## Checks-only job and schedule

A per-run check can't notice an asset that stopped running. So each subject also wires a
**checks-only job** (`build_bronze_checks_job`: `AssetSelection.checks(...)`, which runs the
checks without materializing their assets) and an **hourly UTC schedule**
(`build_bronze_checks_schedule`, `0 * * * *` UTC by default, adjustable per subject; named
`<job>_schedule` unless named explicitly). The schedule **starts running** (`default_status`
RUNNING), so deploying a code location turns its checks on with no manual step. A schedule
someone deliberately stopped in the UI stays stopped: Dagster keeps a schedule's state from the
UI over its code default. A check that fails on its very first run (before an asset's first
capture, for example) is accepted, because it clears itself on a later run. The job has **no concurrency
pool**: checks make no source calls, so they must never compete with, or be stuck behind, the
`*_api` ingestion pools. A job can't span code locations, so there's one per code location.

## Standalone scripts

`scripts/verify_bronze.py` (integrity, validity, consistency, completeness) and
`scripts/sweep_streams.py` (content classification) are the original full-tree, read-only
validators the four families were ported from. They're kept for a manual whole-history sweep,
for example when onboarding a source or re-deriving a subject's expected-empty set. Routine
monitoring uses the checks.

```bash
python3 scripts/verify_bronze.py  "$BRONZE_ROOT"
python3 scripts/sweep_streams.py  "$BRONZE_ROOT"
```

## Decisions & Alternatives

| Decision | Chosen | Alternatives Considered | Rationale |
|---|---|---|---|
| Freshness signal | Newest sidecar fetch time | Dagster native freshness policy on materialization time | Trailing-window assets re-materialize constantly, so materialization time doesn't reflect real captures. Sidecars exist for every capture. |
| Severities | Freshness, schema drift and corruption ERROR; completeness and emptiness WARN | Everything ERROR | Gaps and empties are often legitimate; paging on them would train people to ignore alerts. |
| Schema sample | Richest of the newest payload per recent folder; empties ignored | Newest payload only | Thin, not-yet-synced stub days and empty sparse-collection days caused false drift alerts. |
| Empty baseline | Treated as no baseline and re-recorded | Fail until an operator intervenes | An empty baseline carries no schema; self-healing avoids host file surgery. |
| Baseline location | `BRONZE_MONITOR_DIR`, refusing paths inside bronze; disabled if unset | Under `BRONZE_ROOT` | Bronze stays immutable raw capture only. |
| Paging path | ERROR failures log `asset_check_failed` → Grafana Loki rule → Slack | Dagster+ alerting; email | Dagster OSS has no push alerts; logs already ship to Loki and Grafana already alerts from it. |
| Stopped-asset detection | A checks-only job on an hourly schedule, unpooled | Rely on per-materialization checks | Per-run checks never run for an asset that stopped. Pool-free so ingestion wedges can't hide it. |
| Checks schedule default | Starts running | Starts stopped (Dagster's default), enabled by hand | A safety net that depends on someone remembering to switch it on can silently never run. A first-run failure that clears itself is an acceptable cost. |

## Open Questions & Future Decisions

### Deferred

1. **Unreadable baseline.** A corrupt or malformed baseline file is treated as missing and
   silently re-recorded, which could hide real drift. Failing with an `error` status instead
   would surface it.
2. **Schema sample fallback.** If a folder's newest payload is empty or unreadable, an older,
   non-empty payload in the same folder isn't considered.

## References

- `packages/core/src/grecohome_core/checks/`
- `docs/intent/garmin/capture/capture-design.md`, `docs/intent/whoop/capture/capture-design.md`: each subject's check configuration
