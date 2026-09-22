"""Tests for the USCRN schedule (trailing-window RunRequests)."""

from datetime import UTC, datetime

import pytest
from dagster import build_schedule_context
from grecohome_soil.config import settings
from grecohome_soil.dagster.schedules import (
    PARTITION_RANGE_END_TAG,
    PARTITION_RANGE_START_TAG,
    uscrn_correction_schedule,
    uscrn_schedule,
)


@pytest.mark.unit
class TestUscrnSchedule:
    def test_emits_trailing_lookback_partitions(self):
        ctx = build_schedule_context(scheduled_execution_time=datetime(2026, 6, 9, 12, tzinfo=UTC))
        reqs = list(uscrn_schedule(ctx))

        # Default uscrn_lookback_days = 2; end_offset=1 includes the current day.
        keys = [r.partition_key for r in reqs]
        assert keys == ["2026-06-08", "2026-06-09"]

    def test_run_keys_carry_the_tick(self):
        ctx = build_schedule_context(scheduled_execution_time=datetime(2026, 6, 9, 12, tzinfo=UTC))
        reqs = list(uscrn_schedule(ctx))
        # Distinct per-tick run_key so re-emitting a partition is a new run.
        assert all(r.run_key.endswith("-20260609T12") for r in reqs)
        assert len({r.run_key for r in reqs}) == len(reqs)


@pytest.mark.unit
class TestUscrnCorrectionSchedule:
    def test_emits_one_partition_range_run(self, monkeypatch):
        monkeypatch.setattr(settings, "uscrn_correction_lookback_days", 10)
        ctx = build_schedule_context(
            scheduled_execution_time=datetime(2026, 6, 14, 3, 30, tzinfo=UTC)
        )
        reqs = list(uscrn_correction_schedule(ctx))
        assert len(reqs) == 1
        req = reqs[0]
        assert req.partition_key is None
        assert req.tags[PARTITION_RANGE_START_TAG] == "2026-06-05"
        assert req.tags[PARTITION_RANGE_END_TAG] == "2026-06-14"
        assert req.run_key == "correction-2026-06-05-2026-06-14-20260614T03"

    def test_default_window_covers_the_prior_year_file(self, monkeypatch):
        monkeypatch.delenv("USCRN_CORRECTION_LOOKBACK_DAYS", raising=False)
        ctx = build_schedule_context(
            scheduled_execution_time=datetime(2026, 9, 27, 3, 30, tzinfo=UTC)
        )
        req = next(iter(uscrn_correction_schedule(ctx)))
        assert req.tags[PARTITION_RANGE_START_TAG] < "2026-01-01"
