"""A checks-only job + schedule, so bronze checks also run *independently* of
materialization.

Per-materialization checks can't catch the one failure we most need: an asset that
**stops** materializing. This builds a job that executes the given asset checks
*without* materializing their assets (verified against dagster 1.13.8:
``AssetSelection.checks(...)`` runs the check steps alone), plus a schedule to run
it on a cadence. Each subject wires its own pair into its ``Definitions``.

The job carries **no concurrency pool** — checks make no source-API calls, so they
must never contend with (or be wedged by) the ``*_api`` ingestion pools.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING

from dagster import (
    AssetChecksDefinition,
    AssetSelection,
    ScheduleDefinition,
    define_asset_job,
)

if TYPE_CHECKING:
    from dagster._core.definitions.unresolved_asset_job_definition import (
        UnresolvedAssetJobDefinition,
    )


def build_bronze_checks_job(
    checks: Sequence[AssetChecksDefinition], *, name: str = "bronze_checks_job"
) -> UnresolvedAssetJobDefinition:
    """A job that runs ``checks`` only (their assets are *not* materialized)."""
    return define_asset_job(name=name, selection=AssetSelection.checks(*checks))


def build_bronze_checks_schedule(
    job: UnresolvedAssetJobDefinition,
    *,
    name: str | None = None,
    cron_schedule: str = "0 * * * *",
) -> ScheduleDefinition:
    """An hourly (by default), UTC schedule that runs a checks-only job.

    Hourly is cheap (each check is a bounded, local file read) and catches a stopped
    asset quickly. Override ``cron_schedule`` for lower-frequency subjects.
    """
    return ScheduleDefinition(
        name=name or f"{job.name}_schedule",
        job=job,
        cron_schedule=cron_schedule,
        execution_timezone="UTC",
    )
