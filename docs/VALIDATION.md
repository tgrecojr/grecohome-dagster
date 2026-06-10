# Bronze validation (Dagster asset checks)

Bronze-layer data quality runs *inside Dagster* as `@asset_check`s, so it surfaces
in the UI next to materializations, fails where runs are watched, and catches the
one failure per-materialization checks can't: **an asset that stops capturing.**

Checks are **read-only over `BRONZE_ROOT`** — they never create, modify, or delete
anything under it. The only state they write (schema baselines) lives **outside**
bronze, under `BRONZE_MONITOR_DIR`.

The generic check logic lives in `grecohome-core` (`grecohome_core/checks/`); each
subject only supplies a list of `CollectionCheckConfig` and calls the builders.
This mirrors the core-vs-subject split used for capture.

## The four check families

| Family | Severity | Question | How |
|---|---|---|---|
| **fetch-freshness** | ERROR | Have we captured recently enough? | Newest sidecar `fetched_at` across the collection vs `cadence_hours + grace_hours`. |
| **event-completeness** | WARN | Gaps in the event timeline? | Distinct **event** dates, ordered; flag consecutive gaps `> cadence_days`. |
| **schema-drift** | ERROR | Has the payload's top-level shape changed? | Signature (sorted keys / CSV columns / txt field-count) from **one** payload vs a stored baseline. |
| **content-health** | WARN (ERROR on corruption) | Are payloads carrying real data, and intact? | Classify a recent sample (DATA / EMPTY_* / ERROR_LIKE / HTTP_ERROR / ...) **and** verify sha256/byte_size/parse. |

Severity is deliberate. Freshness and schema drift mean the pipeline or the source
is broken → **ERROR**. Completeness gaps and empty payloads are frequently
legitimate (device not worn, intermittent CGM, hardware-unsupported endpoints) →
**WARN**, so they don't cry wolf. Byte corruption found by content-health is an
**ERROR** (it outranks emptiness).

All checks are `blocking=False`: a failed check surfaces but never aborts the
materialization — capture stays robust. They carry **no concurrency pool**, so they
never contend with (or get wedged by) the `*_api` ingestion pools.

### Why freshness reads sidecars, not Dagster materialization time

Every payload has a sidecar carrying `fetched_at`, whatever the payload's shape, so
the newest sidecar is the one universal "are we still capturing this?" signal across
every source. We use a hand-rolled `@asset_check` (not a native `FreshnessPolicy`)
because native freshness keys off **materialization** records, and our trailing-window
assets re-materialize hourly — the materialization clock would be noisy, and it
wouldn't reflect the actual data-fetch time.

**Dedup caveat.** Capture uses content-hash dedup, so a sidecar's `fetched_at` marks
the last *changed* capture, not the last *fetch attempt*. For collections that
rescore often (Whoop sleep/recovery/cycle/workout) this is fine — content changes
frequently, so writes are frequent. For **near-static, dedup'd** collections (Whoop
`profile` / `body_measurement`) a new file can be weeks apart even though we fetch
hourly, which would make sidecar-freshness false-positive "stale". Freshness is
therefore **disabled** for those snapshots; schema + content health still apply.

## `dt` is not always the event date

Bronze partitions are **UTC fetch-slices**. For some collections `dt` *is* the event
date; for others it's the fetch date. Completeness uses the **true event date**:

| Collection(s) | `dt` means | Event date from |
|---|---|---|
| Whoop sleep/recovery/cycle/workout | partition date | payload field (`start` / `created_at`) |
| Whoop `profile` / `body_measurement` | fetch date | — (current-only snapshot; completeness skipped) |
| Garmin daily collections | partition date | partition `dt` (capture-once/immutable) |
| Lingo `glucose` | **fetch date** | **in-CSV reading timestamp** (NOT `dt`) |
| USCRN `hourly` | partition date | partition `dt` (= the UTC date) |

`CollectionCheckConfig.event_date_source` picks this: `"partition"`, `"payload"`
(then `event_date_field` names the JSON key / CSV column), or `"none"` (snapshot →
completeness skipped entirely).

## Schema-drift baselines (`BRONZE_MONITOR_DIR`)

Baselines are JSON files under `BRONZE_MONITOR_DIR/schema_baselines/<source>/<collection>.json`
— **never** under `BRONZE_ROOT`. The check refuses to write a baseline that would
resolve inside `BRONZE_ROOT`. The first observation records the current signature
(status `baseline_set`, passes); a later change fails ERROR with `was`/`now`. If
`BRONZE_MONITOR_DIR` is unset the check no-ops (status `disabled`, passes) — it never
falls back to writing under bronze. Mount it as a **separate** writable volume (see
`docs/DEPLOYMENT.md`).

## The sidecar gotcha (baked into the shared helper)

`*.meta.json` sidecars sit beside payloads and share the `.json` suffix. Every
payload read in `grecohome_core/checks/bronze_reads.py` excludes them, and schema
signatures read **one exact payload file** (never a glob) so sidecar fields
(`sha256`, `fetched_at`, `stored_encoding`, ...) can never leak into an inferred
signature. There's a regression test asserting exactly this. No subject should
re-implement bronze reads — use the shared helpers.

## Expected-empty collections

Some collections are *legitimately* always-empty on this hardware (e.g. Garmin
`hrv`, `training_readiness` on a device without the sensor). Mark them
`expected_empty=True`: content-health then passes on empty payloads (a genuine
**error** envelope still warns), and freshness does not fail on zero captures.

For Garmin the set is derived two ways, because **the catalog's `skip_if_none` /
`skip_if_empty` flags alone are not enough**: those flags only fire when an endpoint
returns `None`/`{}` (write-nothing). A content sweep of the live tree found several
collections that write an *empty payload daily* instead — `training_readiness`,
`body_battery_events`, `running_tolerance`, `goals`, `max_metrics` (and intermittent
`activities`) — which the flags miss. So `expected_empty` is
`skip_if_none or skip_if_empty or collection in EMPIRICALLY_EMPTY`
(`grecohome_garmin/dagster/checks.py`). Re-run `scripts/sweep_streams.py` and update
`EMPIRICALLY_EMPTY` if the hardware/endpoints change.

## The checks-only validation job (catches a *stopped* asset)

Per-materialization checks can't catch an asset that stops materializing. Each
subject therefore also ships a **checks-only job + hourly schedule** (built by
`grecohome_core.checks.build_bronze_checks_job` / `build_bronze_checks_schedule`)
that runs all of that subject's checks **without materializing the assets**, off the
`*_api` concurrency pools. Jobs: `whoop_bronze_checks_job`, `garmin_bronze_checks_job`,
`lingo_bronze_checks_job`, `uscrn_bronze_checks_job` (each with a `*_hourly` schedule,
off by default — enable in the UI/deploy like the capture schedules). A job can't
span code locations, so there's one per subject rather than a single global job.

## Per-subject status (all wired)

- **Whoop** (`grecohome_whoop/dagster/checks.py`) — sleep/recovery/cycle get all four
  families; workout drops content-health (intermittent — empty windows are normal)
  and uses a wide completeness window; profile/body_measurement get schema + content
  only (freshness disabled per the dedup caveat above). 19 checks.
- **Garmin** (`grecohome_garmin/dagster/checks.py`) — configs **generated from the
  catalog**. Freshness on every collection; completeness/schema/content on the
  analytically-important daily series (`IMPORTANT`); the rest freshness-only (plus
  content where expected-empty). ~124 checks.
- **Lingo** (`grecohome_lingo/dagster/checks.py`) — one glucose collection; completeness
  parses the in-CSV reading timestamp (`Time of Glucose Reading [...]`, confirmed
  against the live tree). **Freshness disabled** — wear is intermittent, so
  "no recent upload" is usually legitimate non-wear, not a break; gaps surface via
  completeness (WARN, wide tolerance). 3 checks.
- **Soil/USCRN** (`grecohome_soil/dagster/checks.py`) — one hourly collection; `dt` is
  the event date; schema-drift uses a text field-count signature. 4 checks.

## Standalone fallback scripts

`scripts/verify_bronze.py` and `scripts/sweep_streams.py` are the original
full-tree, read-only validators (integrity/validity/consistency/completeness, and
content classification, respectively). The four asset-check families above port
their behaviour into Dagster; the scripts remain as a manual, whole-history sweep
(e.g. after onboarding a new source) and are **superseded** for routine monitoring.

```bash
python3 scripts/verify_bronze.py  "$BRONZE_ROOT"
python3 scripts/sweep_streams.py  "$BRONZE_ROOT"
```
