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

from datetime import UTC, datetime

from dagster import AssetCheckResult, AssetCheckSeverity, asset_check

from grecohome_core.checks import (
    CollectionCheckConfig,
    alerting_check,
    build_bronze_checks_job,
    build_bronze_checks_schedule,
    build_collection_checks,
)
from grecohome_core.logging_config import get_logger
from grecohome_core.tokens.file_store import TokenFileStore
from grecohome_whoop.config import settings
from grecohome_whoop.dagster.assets import (
    bronze_cycle,
    bronze_recovery,
    bronze_sleep,
    bronze_snapshots,
    bronze_workout,
)

logger = get_logger(__name__)

# A healthy pipeline refreshes the ~1h access token on every hourly tick, so
# expires_at is never far in the past. Allow ~90 min past expiry (one missed/failed
# refresh cycle) before alerting — a token that merely expired between hourly runs
# (normal, no traffic) must not false-positive. Tune against observed behavior.
_TOKEN_GRACE_SECONDS = 5400


def evaluate_token_health(data: dict | None, now: datetime) -> tuple[bool, dict]:
    """Pure token-health verdict (no I/O), so it's unit-testable.

    Returns ``(passed, metadata)``. Fails when the token file is missing/unparseable
    or when ``expires_at`` is more than ``_TOKEN_GRACE_SECONDS`` in the past.
    """
    if not data or not data.get("expires_at"):
        return False, {"error": "token file missing or has no expires_at"}
    try:
        dt = datetime.fromisoformat(data["expires_at"])
    except (TypeError, ValueError):
        return False, {"error": f"unparseable expires_at: {data.get('expires_at')!r}"}
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    seconds_past_expiry = (now - dt).total_seconds()
    passed = seconds_past_expiry <= _TOKEN_GRACE_SECONDS
    return passed, {
        "expires_at": dt.isoformat(),
        "minutes_past_expiry": round(seconds_past_expiry / 60, 1),
        "grace_minutes": _TOKEN_GRACE_SECONDS // 60,
    }


@asset_check(asset=bronze_sleep, name="whoop_token_health")
@alerting_check
def whoop_token_health() -> AssetCheckResult:
    """ERROR if the Whoop OAuth token is stale — i.e. refresh is failing.

    Reads only the token file (no Whoop API call), so it runs off the ``whoop_api``
    pool in the hourly checks job. A healthy pipeline refreshes hourly; a token
    expired well past the grace window means auth is broken — the silent multi-hour
    outage we want caught fast. On failure it logs ``event=whoop_token_unhealthy``,
    which the Grafana/Loki rule turns into a Slack alert.
    """
    data = TokenFileStore(settings.whoop_token_path).read()
    passed, metadata = evaluate_token_health(data, datetime.now(UTC))
    if not passed:
        logger.warning("whoop_token_unhealthy", **metadata)
    return AssetCheckResult(
        passed=passed, severity=AssetCheckSeverity.ERROR, metadata=metadata
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
# whoop_api ingestion pool. The token-health check (reads only the token file) rides
# in the same checks job so a broken OAuth refresh is caught on the hourly cadence.
whoop_checks = [
    *build_collection_checks(
        WHOOP_CHECK_CONFIGS,
        bronze_root=settings.bronze_root,
        monitor_dir=settings.bronze_monitor_dir,
    ),
    whoop_token_health,
]

# A checks-only job + hourly schedule, so freshness/staleness is caught even if an
# asset stops materializing. Runs off the whoop_api pool (checks make no API calls).
whoop_checks_job = build_bronze_checks_job(whoop_checks, name="whoop_bronze_checks_job")
whoop_checks_schedule = build_bronze_checks_schedule(
    whoop_checks_job, name="whoop_bronze_checks_hourly"
)
