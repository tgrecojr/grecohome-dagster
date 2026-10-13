"""Tests for the run-queue monitor sensor."""

from unittest.mock import patch

import pytest
from dagster import DagsterInstance, SkipReason, build_sensor_context

from grecohome_gold.dagster.monitoring import (
    QUEUE_DEPTH_THRESHOLD,
    queue_verdict,
    run_queue_monitor,
)

pytestmark = pytest.mark.unit


class TestQueueVerdict:
    def test_healthy_below_threshold(self):
        backed_up, msg = queue_verdict(3)
        assert backed_up is False and "healthy" in msg

    def test_at_threshold_is_healthy(self):
        backed_up, _ = queue_verdict(QUEUE_DEPTH_THRESHOLD)
        assert backed_up is False

    def test_above_threshold_backed_up(self):
        backed_up, msg = queue_verdict(QUEUE_DEPTH_THRESHOLD + 5)
        assert backed_up is True and "backed up" in msg


class TestRunQueueMonitorSensor:
    # @spec GOLD-MONITORS-QUEUE-001
    def test_logs_when_backed_up(self):
        inst = DagsterInstance.ephemeral()
        with patch.object(inst, "get_runs_count", return_value=23), patch(
            "grecohome_gold.dagster.monitoring.logger.warning"
        ) as warn:
            result = run_queue_monitor(build_sensor_context(instance=inst))
        assert isinstance(result, SkipReason)  # never launches runs
        warn.assert_called_once()
        assert warn.call_args.args[0] == "run_queue_backed_up"
        assert warn.call_args.kwargs["queued"] == 23

    # @spec GOLD-MONITORS-QUEUE-001
    def test_quiet_when_healthy(self):
        inst = DagsterInstance.ephemeral()
        with patch.object(inst, "get_runs_count", return_value=2), patch(
            "grecohome_gold.dagster.monitoring.logger.warning"
        ) as warn:
            result = run_queue_monitor(build_sensor_context(instance=inst))
        assert isinstance(result, SkipReason)
        warn.assert_not_called()


# --- run_failure_monitor -------------------------------------------------------------

from types import SimpleNamespace  # noqa: E402

from dagster import DefaultSensorStatus  # noqa: E402

from grecohome_gold.dagster.definitions import defs  # noqa: E402


def _run(*, location: str | None = "garmin_ingest", partition: str | None = "2026-10-12"):
    origin = None
    if location is not None:
        origin = SimpleNamespace(repository_origin=SimpleNamespace(
            code_location_origin=SimpleNamespace(location_name=location)))
    tags = {"dagster/partition": partition} if partition else {}
    return SimpleNamespace(job_name="garmin_daily_job", run_id="abc-123", tags=tags,
                           remote_job_origin=origin)


class TestRunFailureMonitor:
    # @spec GOLD-MONITORS-FAILURE-001, GOLD-MONITORS-FAILURE-002
    def test_failed_run_logs_one_line_without_error_text(self):
        from grecohome_gold.dagster.monitoring import log_run_failure

        with patch("grecohome_gold.dagster.monitoring.logger.error") as err:
            log_run_failure(_run())
        err.assert_called_once()
        assert err.call_args.args == ("dagster_run_failed",)
        assert err.call_args.kwargs == {
            "job": "garmin_daily_job", "location": "garmin_ingest",
            "run_id": "abc-123", "partition": "2026-10-12",
        }

    # @spec GOLD-MONITORS-FAILURE-001
    def test_run_without_location_or_partition_logs_nulls(self):
        from grecohome_gold.dagster.monitoring import log_run_failure

        with patch("grecohome_gold.dagster.monitoring.logger.error") as err:
            log_run_failure(_run(location=None, partition=None))
        assert err.call_args.kwargs["location"] is None
        assert err.call_args.kwargs["partition"] is None

    # @spec GOLD-MONITORS-FAILURE-003
    def test_sensor_watches_every_location_and_starts_running(self):
        sensor = defs.get_sensor_def("run_failure_monitor")
        assert sensor.default_status == DefaultSensorStatus.RUNNING
        assert sensor._monitor_all_code_locations is True  # noqa: SLF001


class TestQueueMonitorDefault:
    # @spec GOLD-MONITORS-QUEUE-002
    def test_queue_monitor_starts_running(self):
        sensor = defs.get_sensor_def("run_queue_monitor")
        assert sensor.default_status == DefaultSensorStatus.RUNNING
