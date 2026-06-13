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

    def test_quiet_when_healthy(self):
        inst = DagsterInstance.ephemeral()
        with patch.object(inst, "get_runs_count", return_value=2), patch(
            "grecohome_gold.dagster.monitoring.logger.warning"
        ) as warn:
            result = run_queue_monitor(build_sensor_context(instance=inst))
        assert isinstance(result, SkipReason)
        warn.assert_not_called()
