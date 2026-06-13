"""Bronze-layer data-quality checks as reusable Dagster asset-check builders.

The check *logic* is generic and lives here in ``grecohome-core``; each subject
supplies only its specifics (which collections, each collection's event-date
field, cadence tolerances) via :class:`CollectionCheckConfig` and calls the
builders. This mirrors the existing core-vs-subject split used for capture.

Four families (see :mod:`grecohome_core.checks` submodules):

* :func:`build_fetch_freshness_check` — have we captured recently enough? (ERROR)
* :func:`build_event_completeness_check` — gaps in the event timeline? (WARN)
* :func:`build_schema_drift_check` — has the payload shape changed? (ERROR)
* :func:`build_content_health_check` — are payloads carrying real data, and are
  the bytes intact? (WARN for empty/error share, ERROR for corruption)

All checks read ``BRONZE_ROOT`` strictly read-only and never write under it.
Schema-drift baselines live outside ``BRONZE_ROOT`` (see ``bronze_monitor_dir``).
"""

from dagster import AssetChecksDefinition

from grecohome_core.checks.alerting import alerting_check
from grecohome_core.checks.completeness import build_event_completeness_check
from grecohome_core.checks.config import CollectionCheckConfig
from grecohome_core.checks.content_health import build_content_health_check
from grecohome_core.checks.freshness import build_fetch_freshness_check
from grecohome_core.checks.jobs import (
    build_bronze_checks_job,
    build_bronze_checks_schedule,
)
from grecohome_core.checks.schema_drift import build_schema_drift_check

__all__ = [
    "CollectionCheckConfig",
    "alerting_check",
    "build_bronze_checks_job",
    "build_bronze_checks_schedule",
    "build_checks_for",
    "build_collection_checks",
    "build_content_health_check",
    "build_event_completeness_check",
    "build_fetch_freshness_check",
    "build_schema_drift_check",
]


def build_checks_for(
    cfg: CollectionCheckConfig, bronze_root: str, monitor_dir: str | None = None
) -> list[AssetChecksDefinition]:
    """Build the enabled checks for one collection, honouring ``enabled_checks``.

    Completeness is silently skipped for current-only snapshots
    (``event_date_source == "none"``) even if listed in ``enabled_checks``.
    """
    checks: list[AssetChecksDefinition] = []
    if "freshness" in cfg.enabled_checks:
        checks.append(build_fetch_freshness_check(cfg, bronze_root))
    if "schema" in cfg.enabled_checks:
        checks.append(build_schema_drift_check(cfg, bronze_root, monitor_dir))
    if "completeness" in cfg.enabled_checks and cfg.event_date_source != "none":
        checks.append(build_event_completeness_check(cfg, bronze_root))
    if "content" in cfg.enabled_checks:
        checks.append(build_content_health_check(cfg, bronze_root))
    return checks


def build_collection_checks(
    configs: list[CollectionCheckConfig], bronze_root: str, monitor_dir: str | None = None
) -> list[AssetChecksDefinition]:
    """Flatten :func:`build_checks_for` across a subject's collection configs."""
    out: list[AssetChecksDefinition] = []
    for cfg in configs:
        out.extend(build_checks_for(cfg, bronze_root, monitor_dir))
    return out

