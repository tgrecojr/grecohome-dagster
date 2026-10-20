"""A checks-only job + schedule, so bronze checks also run *independently* of
materialization.

Per-materialization checks can't catch the one failure we most need: an asset that
**stops** materializing. This builds a job that executes the given asset checks
*without* materializing their assets (``AssetSelection.checks(...)`` runs the check
steps alone), plus a schedule to run it on a cadence. Each code location wires its
own pair into its ``Definitions``.

The job runs **in-process**: a check is a sub-second local read, and a per-step
subprocess would spend nearly all of its CPU importing the code location, on every
core at once. It carries **no concurrency pool** — checks make no source-API calls,
so they must never contend with (or be wedged by) the ``*_api`` ingestion pools.

The schedule's cron is **chosen per code location** (no default), and its minute
field must be one literal minute 1–59: the host's own cron jobs land on minute 0,
and every checks job landing there too is a CPU stampede. Each code location takes
its own off-minute (listed in ``docs/DEPLOYMENT.md``).
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING

from dagster import (
    AssetChecksDefinition,
    AssetSelection,
    DefaultScheduleStatus,
    ScheduleDefinition,
    define_asset_job,
    in_process_executor,
)

if TYPE_CHECKING:
    from dagster._core.definitions.unresolved_asset_job_definition import (
        UnresolvedAssetJobDefinition,
    )


# @spec CORE-CHECKS-JOB-001, CORE-CHECKS-JOB-003
def build_bronze_checks_job(
    checks: Sequence[AssetChecksDefinition], *, name: str = "bronze_checks_job"
) -> UnresolvedAssetJobDefinition:
    """A job that runs ``checks`` only (their assets are *not* materialized), in-process."""
    return define_asset_job(
        name=name, selection=AssetSelection.checks(*checks), executor_def=in_process_executor
    )


# @spec CORE-CHECKS-JOB-005
def _require_literal_off_minute(cron_schedule: str) -> None:
    """Reject a cron unless its minute field is one literal minute from 1 to 59."""
    fields = cron_schedule.split()
    minute = fields[0] if fields else ""
    if not (minute.isdecimal() and 1 <= int(minute) <= 59):
        raise ValueError(
            "a checks schedule's cron must name one literal minute from 1 to 59 "
            f"(never minute 0, '*', a step or a list); got {cron_schedule!r}"
        )


# @spec CORE-CHECKS-JOB-002, CORE-CHECKS-JOB-004
def build_bronze_checks_schedule(
    job: UnresolvedAssetJobDefinition,
    *,
    cron_schedule: str,
    name: str | None = None,
) -> ScheduleDefinition:
    """A UTC schedule that runs a checks-only job on the given cron.

    ``cron_schedule`` is required and must tick on a literal off-minute (see the
    module docstring). Match the cadence to what the checks can detect: freshness
    only fails once a capture is older than its tolerance, so checking much more
    often than the subject captures buys CPU, not earlier detection.

    Starts RUNNING: a safety net that waits for someone to switch it on can silently
    never run. A schedule stopped deliberately in the UI stays stopped (the UI state
    wins over this code default).
    """
    _require_literal_off_minute(cron_schedule)
    return ScheduleDefinition(
        name=name or f"{job.name}_schedule",
        job=job,
        cron_schedule=cron_schedule,
        execution_timezone="UTC",
        default_status=DefaultScheduleStatus.RUNNING,
    )
