"""Bronze data-quality checks for the Whoop code location.

Each Whoop bronze collection gets a :class:`CollectionCheckConfig`; the generic
builders in ``grecohome_core.checks`` turn those into Dagster ``@asset_check``s
attached to the existing bronze assets. We only describe Whoop's specifics here —
which collections, each collection's event-date field, cadence tolerances — and
never reimplement check logic.

Whoop specifics worth knowing:

* **Range collections** (sleep/recovery/workout/cycle) are daily-partitioned and
  *rescore*, so the latest partitions are re-captured (and re-written) frequently;
  sidecar freshness tracks that cleanly. Their event date lives in the payload
  (``start`` / ``created_at``), not the partition ``dt``.
* **Snapshots** (profile, body_measurement) are current-only and captured with
  content-hash dedup, so a new file is written only when the snapshot *changes* —
  which for these near-static collections can be weeks apart. Sidecar freshness
  would therefore false-positive "stale", so freshness is intentionally disabled
  for snapshots; schema + content health still apply. They share one asset
  (``whoop_bronze_snapshots``); two configs attach to it, one per collection.
* **Workouts** are intermittent (the user had a real multi-week gap), so the
  completeness window is wide and WARN-only, and content health is skipped (empty
  capture windows are normal and would otherwise cry wolf).
"""

from __future__ import annotations

from grecohome_core.checks import (
    CollectionCheckConfig,
    build_bronze_checks_job,
    build_bronze_checks_schedule,
    build_collection_checks,
)
from grecohome_whoop.config import settings
from grecohome_whoop.dagster.assets import (
    bronze_cycle,
    bronze_recovery,
    bronze_sleep,
    bronze_snapshots,
    bronze_workout,
)

# Range collections: daily-partitioned, {"records": [...]} payloads, event date
# from the payload (dt is the partition date here, but completeness counts the
# distinct event days the records actually cover).
WHOOP_CHECK_CONFIGS: list[CollectionCheckConfig] = [
    CollectionCheckConfig(
        source="whoop",
        collection="sleep",
        asset_key=bronze_sleep.key,
        reader="json",
        unnest_records=True,
        event_date_source="payload",
        event_date_field="start",
        cadence_hours=26.0,
        cadence_days=2,
    ),
    CollectionCheckConfig(
        source="whoop",
        collection="recovery",
        asset_key=bronze_recovery.key,
        reader="json",
        unnest_records=True,
        event_date_source="payload",
        event_date_field="created_at",
        cadence_hours=26.0,
        cadence_days=2,
    ),
    CollectionCheckConfig(
        source="whoop",
        collection="cycle",
        asset_key=bronze_cycle.key,
        reader="json",
        unnest_records=True,
        event_date_source="payload",
        event_date_field="start",
        cadence_hours=26.0,
        cadence_days=2,
    ),
    # Workouts are intermittent: wide completeness window, WARN-only, and no
    # content-health (empty fetch windows are normal, not a problem).
    CollectionCheckConfig(
        source="whoop",
        collection="workout",
        asset_key=bronze_workout.key,
        reader="json",
        unnest_records=True,
        event_date_source="payload",
        event_date_field="start",
        cadence_hours=26.0,
        cadence_days=14,
        enabled_checks=frozenset({"freshness", "completeness", "schema"}),
    ),
    # Snapshots: current-only, dedup'd -> freshness disabled (see module docstring).
    CollectionCheckConfig(
        source="whoop",
        collection="profile",
        asset_key=bronze_snapshots.key,
        reader="json",
        unnest_records=False,
        event_date_source="none",
        enabled_checks=frozenset({"schema", "content"}),
    ),
    CollectionCheckConfig(
        source="whoop",
        collection="body_measurement",
        asset_key=bronze_snapshots.key,
        reader="json",
        unnest_records=False,
        event_date_source="none",
        enabled_checks=frozenset({"schema", "content"}),
    ),
]

# The concrete asset checks to register in the Whoop Definitions. These read bronze
# read-only and carry no concurrency pool, so they never contend with the
# whoop_api ingestion pool.
whoop_checks = build_collection_checks(
    WHOOP_CHECK_CONFIGS,
    bronze_root=settings.bronze_root,
    monitor_dir=settings.bronze_monitor_dir,
)

# A checks-only job + hourly schedule, so freshness/staleness is caught even if an
# asset stops materializing. Runs off the whoop_api pool (checks make no API calls).
whoop_checks_job = build_bronze_checks_job(whoop_checks, name="whoop_bronze_checks_job")
whoop_checks_schedule = build_bronze_checks_schedule(
    whoop_checks_job, name="whoop_bronze_checks_hourly"
)
