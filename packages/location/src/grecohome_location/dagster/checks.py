"""Bronze data-quality checks for the location code location.

Two collections (``location/overland``, ``location/owntracks``), each a byte-exact
raw JSON POST body. Location is **event-driven** (phone off / travel / stationary
batching all produce legitimate gaps), and ``dt`` is the *receipt* date, so the check
mix differs from the API-polling subjects:

* **Content health** (core, WARN) — both streams: payloads parse and carry data.
* **Schema drift** (core, ERROR) — **overland only**: its body is a stable
  ``{"locations":[…]}`` (signature ``["locations"]``). OwnTracks messages are
  polymorphic (``_type`` = location/transition/lwt, plus optional keys), so the
  richest-payload signature would churn false ERRORs — schema drift is skipped there.
* **Receipt freshness** (custom, WARN→ERROR) — hours since the newest
  ``received_unix_ms`` in bronze. WARN wide; ERROR only past a very long gap. Core
  freshness is ERROR-only and measures *promote* time, so it can't express this.
* **Promote lag** (custom, ERROR) — no staging file older than the threshold remains
  un-promoted, proving the promoter keeps up before relay retention prunes staging.

Checks read bronze/staging strictly read-only and run on the checks-only job (no
API/promote pool). ``@alerting_check`` emits a page only on a failing ERROR result;
WARN stays UI-only.
"""

from __future__ import annotations

from datetime import UTC, datetime

from dagster import (
    AssetCheckResult,
    AssetChecksDefinition,
    AssetCheckSeverity,
    asset_check,
)

from grecohome_core.checks import (
    CollectionCheckConfig,
    build_bronze_checks_job,
    build_bronze_checks_schedule,
    build_collection_checks,
)
from grecohome_core.checks.alerting import alerting_check
from grecohome_core.checks.bronze_reads import (
    collection_dir,
    iter_payloads,
    read_sidecar,
    trailing_partition_dirs,
)
from grecohome_core.logging_config import get_logger
from grecohome_location.capture import SOURCE, iso_from_ms
from grecohome_location.config import settings
from grecohome_location.dagster.assets import (
    location_bronze_overland,
    location_bronze_owntracks,
)
from grecohome_location.promote import unpromoted_staging

logger = get_logger(__name__)

_STREAM_ASSETS = {
    "overland": location_bronze_overland,
    "owntracks": location_bronze_owntracks,
}


# ---------------------------------------------------------------------------
# Core checks (content for both; schema drift for the stable overland stream)
# ---------------------------------------------------------------------------
LOCATION_CHECK_CONFIGS: list[CollectionCheckConfig] = [
    CollectionCheckConfig(
        source=SOURCE,
        collection="overland",
        asset_key=location_bronze_overland.key,
        reader="json",
        event_date_source="none",  # dt = receipt date; no event timeline to complete
        enabled_checks=frozenset({"content", "schema"}),
        recent_partitions=settings.location_recent_partitions,
    ),
    CollectionCheckConfig(
        source=SOURCE,
        collection="owntracks",
        asset_key=location_bronze_owntracks.key,
        reader="json",
        event_date_source="none",
        # Schema drift skipped: polymorphic OwnTracks messages would false-positive.
        enabled_checks=frozenset({"content"}),
        recent_partitions=settings.location_recent_partitions,
    ),
]


# ---------------------------------------------------------------------------
# Custom: receipt freshness (WARN wide, ERROR only past a very long gap)
# ---------------------------------------------------------------------------
def _newest_received_ms(bronze_root: str, stream: str, recent_partitions: int) -> int | None:
    """Newest ``received_unix_ms`` in bronze for ``stream`` (None if no captures).

    Walks partitions newest→oldest and stops at the first with payloads: the newest
    receipt lives in the newest non-empty partition, so this bounds sidecar reads to
    ~one day rather than the whole trailing window.
    """
    coll_dir = collection_dir(bronze_root, SOURCE, stream)
    for _dt, pdir in reversed(trailing_partition_dirs(coll_dir, recent_partitions)):
        payloads = iter_payloads(pdir)
        if not payloads:
            continue
        newest = 0
        for payload in payloads:
            ms = (read_sidecar(payload) or {}).get("received_unix_ms")
            if isinstance(ms, int) and ms > newest:
                newest = ms
        return newest or None
    return None


def build_receipt_freshness_check(stream: str) -> AssetChecksDefinition:
    """WARN when the newest received POST is older than the warn tolerance; ERROR past
    the (much wider) error tolerance. Event-driven data gaps are legitimate, so this
    stays quiet for normal travel/phone-off gaps."""
    asset = _STREAM_ASSETS[stream]
    warn_h = settings.location_freshness_warn_hours
    err_h = settings.location_freshness_error_hours

    @asset_check(
        asset=asset.key,
        name=f"location_{stream}_receipt_freshness",
        blocking=False,
        description=(
            f"Newest {SOURCE}/{stream} receipt within {warn_h:g}h (WARN) / {err_h:g}h (ERROR)."
        ),
    )
    @alerting_check(name=f"location_{stream}_receipt_freshness", asset=asset.key)
    def _check() -> AssetCheckResult:
        try:
            newest = _newest_received_ms(
                settings.bronze_root, stream, settings.location_recent_partitions
            )
            if newest is None:
                # No captures in the window at all: past even the ERROR horizon.
                return AssetCheckResult(
                    passed=False,
                    severity=AssetCheckSeverity.ERROR,
                    metadata={"last_received": "none", "hours_since": -1.0},
                    description="No location receipts in the recent window.",
                )
            hours = (datetime.now(UTC).timestamp() * 1000 - newest) / 3_600_000
            if hours > err_h:
                sev, passed = AssetCheckSeverity.ERROR, False
            elif hours > warn_h:
                sev, passed = AssetCheckSeverity.WARN, False
            else:
                sev, passed = AssetCheckSeverity.WARN, True
            return AssetCheckResult(
                passed=passed,
                severity=sev,
                metadata={
                    "last_received": iso_from_ms(newest),
                    "hours_since": round(hours, 2),
                    "warn_hours": warn_h,
                    "error_hours": err_h,
                },
                description=(
                    f"Fresh: {hours:.1f}h since newest receipt."
                    if passed
                    else f"Stale: {hours:.1f}h since newest receipt."
                ),
            )
        except Exception as e:  # noqa: BLE001 - a check must never break the run
            logger.warning("receipt-freshness check errored", stream=stream, error=str(e))
            return AssetCheckResult(
                passed=False,
                severity=AssetCheckSeverity.ERROR,
                metadata={"error": str(e)},
                description="Receipt-freshness check errored internally.",
            )

    return _check


# ---------------------------------------------------------------------------
# Custom: promote lag (ERROR when the promoter falls behind)
# ---------------------------------------------------------------------------
def build_promote_lag_check(stream: str) -> AssetChecksDefinition:
    """ERROR when a staging file older than the lag threshold is still un-promoted —
    the guardrail that the promoter keeps up before relay retention prunes staging."""
    asset = _STREAM_ASSETS[stream]
    lag_h = settings.location_promote_lag_hours

    @asset_check(
        asset=asset.key,
        name=f"location_{stream}_promote_lag",
        blocking=False,
        description=(
            f"No {SOURCE}/{stream} staging file older than {lag_h:g}h remains un-promoted."
        ),
    )
    @alerting_check(name=f"location_{stream}_promote_lag", asset=asset.key)
    def _check() -> AssetCheckResult:
        try:
            now = datetime.now(UTC)
            todo = unpromoted_staging(
                capture_dir=settings.relay_capture_dir,
                bronze_root=settings.bronze_root,
                state_dir=settings.location_state_dir,
                stream=stream,
                now=now,
                window_days=settings.location_promote_window_days,
            )
            if not todo:
                return AssetCheckResult(
                    passed=True,
                    severity=AssetCheckSeverity.ERROR,
                    metadata={"unpromoted": 0, "oldest_lag_hours": 0.0},
                    description="Caught up: no un-promoted staging files.",
                )
            oldest_ms = min(f.received_ms for f in todo)
            lag_hours = (now.timestamp() * 1000 - oldest_ms) / 3_600_000
            passed = lag_hours <= lag_h
            return AssetCheckResult(
                passed=passed,
                severity=AssetCheckSeverity.ERROR,
                metadata={
                    "unpromoted": len(todo),
                    "oldest_lag_hours": round(lag_hours, 2),
                    "threshold_hours": lag_h,
                    "oldest_received": iso_from_ms(oldest_ms),
                },
                description=(
                    f"{len(todo)} un-promoted; oldest {lag_hours:.1f}h "
                    + ("within" if passed else "OVER")
                    + f" {lag_h:g}h threshold."
                ),
            )
        except Exception as e:  # noqa: BLE001 - a check must never break the run
            logger.warning("promote-lag check errored", stream=stream, error=str(e))
            return AssetCheckResult(
                passed=False,
                severity=AssetCheckSeverity.ERROR,
                metadata={"error": str(e)},
                description="Promote-lag check errored internally.",
            )

    return _check


# ---------------------------------------------------------------------------
# Assemble + wire the checks-only job and schedule
# ---------------------------------------------------------------------------
location_checks: list[AssetChecksDefinition] = [
    *build_collection_checks(
        LOCATION_CHECK_CONFIGS,
        bronze_root=settings.bronze_root,
        monitor_dir=settings.bronze_monitor_dir,
    ),
    *[build_receipt_freshness_check(s) for s in _STREAM_ASSETS],
    *[build_promote_lag_check(s) for s in _STREAM_ASSETS],
]

location_checks_job = build_bronze_checks_job(
    location_checks, name="location_bronze_checks_job"
)
location_checks_schedule = build_bronze_checks_schedule(
    location_checks_job, name="location_bronze_checks_hourly"
)
