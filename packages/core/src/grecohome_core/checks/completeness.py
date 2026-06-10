"""Event-completeness check: gaps in the event timeline beyond the cadence.

Severity is WARN, deliberately: gaps are frequently legitimate (device not worn,
sensor off, hardware unsupported, a real multi-week break). We surface them so
they're visible, but never fail the run over them — a crying-wolf check gets
ignored.

The check counts **distinct event dates**, which is *not* always the partition
``dt``: for Lingo glucose and Whoop snapshots ``dt`` is the fetch date, so the true
event date comes from inside the payload (see ``event_date_source``/``event_date_field``
on :class:`CollectionCheckConfig`). Collections whose event date is "none"
(current-only snapshots) skip this check entirely.
"""

from __future__ import annotations

from dagster import (
    AssetCheckResult,
    AssetChecksDefinition,
    AssetCheckSeverity,
    asset_check,
)

from grecohome_core.checks.bronze_reads import collection_dir, distinct_event_dates, find_gaps
from grecohome_core.checks.config import CollectionCheckConfig
from grecohome_core.logging_config import get_logger

logger = get_logger(__name__)


def build_event_completeness_check(
    cfg: CollectionCheckConfig, bronze_root: str
) -> AssetChecksDefinition:
    """An ``@asset_check`` (WARN) that surfaces consecutive event-date gaps larger
    than ``cadence_days``. Passes when the timeline is dense enough or when there
    aren't yet two event dates to compare.
    """

    @asset_check(
        asset=cfg.asset_key,
        name=f"{cfg.check_name_prefix}_completeness",
        blocking=False,
        description=(
            f"No gaps over {cfg.cadence_days}d in the {cfg.source}/{cfg.collection} "
            "event timeline."
        ),
    )
    def _check() -> AssetCheckResult:
        try:
            if cfg.event_date_source == "none":
                return AssetCheckResult(
                    passed=True,
                    severity=AssetCheckSeverity.WARN,
                    metadata={"status": "skipped", "reason": "current-only snapshot"},
                    description="Completeness not applicable to a current-only snapshot.",
                )

            coll_dir = collection_dir(bronze_root, cfg.source, cfg.collection)
            dates = distinct_event_dates(
                coll_dir,
                event_date_source=cfg.event_date_source,
                event_date_field=cfg.event_date_field,
                reader=cfg.reader,
                unnest_records=cfg.unnest_records,
                recent_partitions=cfg.recent_partitions,
            )

            base_meta = {
                "event_days": len(dates),
                "earliest": dates[0].isoformat() if dates else "none",
                "latest": dates[-1].isoformat() if dates else "none",
            }

            if len(dates) < 2:
                return AssetCheckResult(
                    passed=True,
                    severity=AssetCheckSeverity.WARN,
                    metadata={**base_meta, "gaps_over_cadence": 0, "biggest_gap_days": 0},
                    description="Too few event dates to assess gaps.",
                )

            gaps = find_gaps(dates, cfg.cadence_days)
            biggest = max((missing for _a, _b, missing in gaps), default=0)
            passed = not gaps
            detail = (
                "; ".join(f"{missing}d gap {a}→{b}" for a, b, missing in gaps[:5])
                if gaps
                else "no gaps over cadence"
            )
            return AssetCheckResult(
                passed=passed,
                severity=AssetCheckSeverity.WARN,
                metadata={
                    **base_meta,
                    "gaps_over_cadence": len(gaps),
                    "biggest_gap_days": biggest,
                    "gaps": detail,
                },
                description=(
                    f"{len(gaps)} gap(s) over {cfg.cadence_days}d (largest {biggest}d) — "
                    "may be legitimate (device not worn)."
                    if gaps
                    else f"Timeline dense over {len(dates)} event day(s)."
                ),
            )
        except Exception as e:  # noqa: BLE001 - a check must never break the run
            logger.warning(
                "completeness check errored",
                source=cfg.source,
                collection=cfg.collection,
                error=str(e),
            )
            return AssetCheckResult(
                passed=False,
                severity=AssetCheckSeverity.WARN,
                metadata={"error": str(e)},
                description="Completeness check errored internally.",
            )

    return _check
