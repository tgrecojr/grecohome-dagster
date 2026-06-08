"""Tests for the shared Dagster helpers."""

from datetime import UTC, datetime

import pytest

from grecohome_core.dagster.helpers import (
    daily_utc_partitions,
    run_async,
    trailing_partition_keys,
)


@pytest.mark.unit
class TestDailyUtcPartitions:
    def test_is_utc(self):
        pd = daily_utc_partitions("2024-01-01")
        assert str(pd.timezone) == "UTC"

    def test_keys_are_date_strings(self):
        pd = daily_utc_partitions("2024-01-01")
        before = datetime(2024, 1, 5, 12, tzinfo=UTC)
        keys = pd.get_partition_keys(current_time=before)
        assert keys[0] == "2024-01-01"


@pytest.mark.unit
class TestTrailingPartitionKeys:
    def test_includes_current_in_progress_day(self):
        pd = daily_utc_partitions("2024-01-01")
        before = datetime(2024, 1, 10, 5, 0, tzinfo=UTC)
        keys = trailing_partition_keys(pd, before, count=8)
        assert len(keys) == 8
        assert keys[-1] == "2024-01-10"  # the in-progress day is included
        assert keys[0] == "2024-01-03"  # 8 trailing days ending on the 10th

    def test_clamps_to_available_history(self):
        pd = daily_utc_partitions("2024-01-01")
        before = datetime(2024, 1, 3, 5, 0, tzinfo=UTC)
        keys = trailing_partition_keys(pd, before, count=8)
        # Only 3 partitions exist yet (Jan 1-3); count is clamped by availability.
        assert keys == ["2024-01-01", "2024-01-02", "2024-01-03"]


@pytest.mark.unit
class TestRunAsync:
    def test_runs_coroutine_to_completion(self):
        async def coro():
            return 42

        assert run_async(coro()) == 42
