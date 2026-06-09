"""Tests for the USCRN schedule (trailing-window RunRequests)."""

from datetime import UTC, datetime

import pytest
from dagster import build_schedule_context
from grecohome_soil.dagster.schedules import uscrn_schedule


@pytest.mark.unit
class TestUscrnSchedule:
    def test_emits_trailing_lookback_partitions(self):
        ctx = build_schedule_context(
            scheduled_execution_time=datetime(2026, 6, 9, 12, tzinfo=UTC)
        )
        reqs = list(uscrn_schedule(ctx))

        # Default uscrn_lookback_days = 2; end_offset=1 includes the current day.
        keys = [r.partition_key for r in reqs]
        assert keys == ["2026-06-08", "2026-06-09"]

    def test_run_keys_carry_the_tick(self):
        ctx = build_schedule_context(
            scheduled_execution_time=datetime(2026, 6, 9, 12, tzinfo=UTC)
        )
        reqs = list(uscrn_schedule(ctx))
        # Distinct per-tick run_key so re-emitting a partition is a new run.
        assert all(r.run_key.endswith("-20260609T12") for r in reqs)
        assert len({r.run_key for r in reqs}) == len(reqs)
