"""Fetch-freshness check: have we captured this collection recently enough?

The one universal signal: every payload has a sidecar with ``fetched_at`` whatever
the payload's shape, so the newest sidecar across a collection answers "are we
still capturing this?" uniformly for every source. Severity is ERROR — staleness
means the pipeline or the source is broken, which is exactly the failure a stopped
asset produces and the one we most need to catch.
"""

from __future__ import annotations

from datetime import UTC, datetime

from dagster import (
    AssetCheckResult,
    AssetChecksDefinition,
    AssetCheckSeverity,
    asset_check,
)

from grecohome_core.checks.alerting import alerting_check
from grecohome_core.checks.bronze_reads import collection_dir, newest_fetch
from grecohome_core.checks.config import CollectionCheckConfig
from grecohome_core.logging_config import get_logger

logger = get_logger(__name__)


def build_fetch_freshness_check(
    cfg: CollectionCheckConfig, bronze_root: str
) -> AssetChecksDefinition:
    """An ``@asset_check`` that fails (ERROR) when a collection's newest capture is
    stale beyond ``cadence_hours + grace_hours``, or when a configured collection has
    no captures at all.

    A collection marked ``expected_empty`` passes when it has zero captures (those
    endpoints legitimately write nothing on unsupported hardware); once it *does*
    capture, normal staleness applies.
    """
    tolerance = cfg.cadence_hours + cfg.grace_hours

    @asset_check(
        asset=cfg.asset_key,
        name=f"{cfg.check_name_prefix}_freshness",
        blocking=False,
        description=(
            f"Newest {cfg.source}/{cfg.collection} capture is within "
            f"{tolerance:g}h ({cfg.cadence_hours:g}h cadence + {cfg.grace_hours:g}h grace)."
        ),
    )
    @alerting_check(name=f"{cfg.check_name_prefix}_freshness", asset=cfg.asset_key)
    def _check() -> AssetCheckResult:
        try:
            coll_dir = collection_dir(bronze_root, cfg.source, cfg.collection)
            last_fetch, sidecar_count = newest_fetch(coll_dir, cfg.recent_partitions)

            if sidecar_count == 0 or last_fetch is None:
                passed = cfg.expected_empty
                return AssetCheckResult(
                    passed=passed,
                    severity=AssetCheckSeverity.ERROR,
                    metadata={
                        "last_fetch": "none",
                        "hours_since": -1.0,
                        "tolerance_hours": tolerance,
                        "sidecar_count": 0,
                    },
                    description=(
                        "No captures in the recent window for an expected-empty "
                        "collection (acceptable)."
                        if passed
                        else "No captures found for a configured collection."
                    ),
                )

            hours_since = (datetime.now(UTC) - last_fetch).total_seconds() / 3600
            passed = hours_since <= tolerance
            return AssetCheckResult(
                passed=passed,
                severity=AssetCheckSeverity.ERROR,
                metadata={
                    "last_fetch": last_fetch.isoformat(),
                    "hours_since": round(hours_since, 2),
                    "tolerance_hours": tolerance,
                    "sidecar_count": sidecar_count,
                },
                description=(
                    f"Fresh: {hours_since:.1f}h since last capture."
                    if passed
                    else f"Stale: {hours_since:.1f}h since last capture (> {tolerance:g}h)."
                ),
            )
        except Exception as e:  # noqa: BLE001 - a check must never break the run
            logger.warning(
                "freshness check errored",
                source=cfg.source,
                collection=cfg.collection,
                error=str(e),
            )
            return AssetCheckResult(
                passed=False,
                severity=AssetCheckSeverity.ERROR,
                metadata={"error": str(e)},
                description="Freshness check errored internally.",
            )

    return _check
