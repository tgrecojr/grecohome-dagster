"""Instance-level run monitors: failed runs and a backed-up run queue.

``run_failure_monitor`` logs ``dagster_run_failed`` for every failed run in any code
location, so a failure pages when it happens instead of when a freshness check notices.
The line carries the job, code location, run id and partition only, never error text.


The pagination-wedge incident showed the failure mode no per-asset check sees: a run
holds a concurrency pool and the hourly schedules pile up behind it. This sensor reads
the **shared Dagster instance's** queued-run count directly (Dagster's own instance
access — no external Postgres reader) and, when it stays high, logs a structured
``run_queue_backed_up`` line that the existing Grafana/Loki → Slack path turns into an
alert.

It only observes — it never launches runs (always returns ``SkipReason``). Lives in the
gold code location (top of the stack, lightest) but sees the *whole* instance's queue.
Both start running: a safety net that depends on someone enabling it can silently never run.
"""

from __future__ import annotations

from dagster import (
    DagsterRunStatus,
    DefaultSensorStatus,
    RunFailureSensorContext,
    RunsFilter,
    SensorEvaluationContext,
    SkipReason,
    run_failure_sensor,
    sensor,
)

from grecohome_core.logging_config import get_logger

logger = get_logger(__name__)

# Above a normal top-of-hour burst (several schedules emit trailing-window runs that
# drain through the per-source pools in a few minutes). Paired with the Grafana rule's
# long `for:` so a brief burst that drains doesn't page — only a sustained backup does.
QUEUE_DEPTH_THRESHOLD = 15


def queue_verdict(queued: int, *, threshold: int = QUEUE_DEPTH_THRESHOLD) -> tuple[bool, str]:
    """Pure verdict (testable): is the queue backed up, and the status message."""
    if queued > threshold:
        return True, f"Run queue backed up: {queued} queued (> {threshold})."
    return False, f"Run queue healthy: {queued} queued."


# @spec GOLD-MONITORS-QUEUE-001, GOLD-MONITORS-QUEUE-002
@sensor(
    name="run_queue_monitor",
    minimum_interval_seconds=60,
    default_status=DefaultSensorStatus.RUNNING,
    description="Alerts (via Loki/Slack) when the Dagster run queue stays backed up.",
)
def run_queue_monitor(context: SensorEvaluationContext) -> SkipReason:
    """Log ``run_queue_backed_up`` when queued runs exceed the threshold; never launches."""
    queued = context.instance.get_runs_count(RunsFilter(statuses=[DagsterRunStatus.QUEUED]))
    backed_up, message = queue_verdict(queued)
    if backed_up:
        logger.warning("run_queue_backed_up", queued=queued, threshold=QUEUE_DEPTH_THRESHOLD)
    return SkipReason(message)


def _location(run) -> str | None:
    """The run's code location name, or None for a run launched outside one (e.g. the CLI)."""
    origin = getattr(run, "remote_job_origin", None)
    if origin is None:
        return None
    return origin.repository_origin.code_location_origin.location_name


def log_run_failure(run) -> None:
    """Log one ``dagster_run_failed`` line for a failed run: identifiers only, no error text."""
    logger.error(
        "dagster_run_failed",
        job=run.job_name,
        location=_location(run),
        run_id=run.run_id,
        partition=(run.tags or {}).get("dagster/partition"),
    )


# @spec GOLD-MONITORS-FAILURE-001, GOLD-MONITORS-FAILURE-002, GOLD-MONITORS-FAILURE-003
@run_failure_sensor(
    name="run_failure_monitor",
    monitor_all_code_locations=True,
    default_status=DefaultSensorStatus.RUNNING,
    description="Alerts (via Loki/Slack) on every failed run in any code location.",
)
def run_failure_monitor(context: RunFailureSensorContext) -> None:
    """Log ``dagster_run_failed`` for the failed run; never launches runs."""
    log_run_failure(context.dagster_run)
