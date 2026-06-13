"""Instance-level run-queue monitor (catches runs backing up).

The pagination-wedge incident showed the failure mode no per-asset check sees: a run
holds a concurrency pool and the hourly schedules pile up behind it. This sensor reads
the **shared Dagster instance's** queued-run count directly (Dagster's own instance
access — no external Postgres reader) and, when it stays high, logs a structured
``run_queue_backed_up`` line that the existing Grafana/Loki → Slack path turns into an
alert.

It only observes — it never launches runs (always returns ``SkipReason``). Lives in the
gold code location (top of the stack, lightest) but sees the *whole* instance's queue.
Off by default; enable it in the UI like the schedules.
"""

from __future__ import annotations

from dagster import (
    DagsterRunStatus,
    DefaultSensorStatus,
    RunsFilter,
    SensorEvaluationContext,
    SkipReason,
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


@sensor(
    name="run_queue_monitor",
    minimum_interval_seconds=60,
    default_status=DefaultSensorStatus.STOPPED,
    description="Alerts (via Loki/Slack) when the Dagster run queue stays backed up.",
)
def run_queue_monitor(context: SensorEvaluationContext) -> SkipReason:
    """Log ``run_queue_backed_up`` when queued runs exceed the threshold; never launches."""
    queued = context.instance.get_runs_count(RunsFilter(statuses=[DagsterRunStatus.QUEUED]))
    backed_up, message = queue_verdict(queued)
    if backed_up:
        logger.warning("run_queue_backed_up", queued=queued, threshold=QUEUE_DEPTH_THRESHOLD)
    return SkipReason(message)
