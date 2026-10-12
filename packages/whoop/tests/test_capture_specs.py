"""Tests for Whoop capture behavior pinned by the capture specs.

Covers the request window, non-JSON responses, materialization metadata, snapshot
order and folder, retry waits and the rate limiter, and the bronze-check wiring.
"""

import os
from datetime import UTC, datetime
from unittest.mock import AsyncMock, Mock, patch

import httpx
import pytest
import respx
from dagster import materialize

from grecohome_whoop.api.whoop_client import (
    WhoopAPIError,
    WhoopClient,
    WhoopRetryableError,
    _whoop_retry_wait,
)
from grecohome_whoop.config import settings
from grecohome_whoop.dagster.assets import bronze_sleep, bronze_snapshots
from grecohome_whoop.dagster.checks import WHOOP_CHECK_CONFIGS, whoop_checks, whoop_token_health
from grecohome_whoop.dagster.definitions import defs

TOKEN_PATCH = "grecohome_whoop.auth.token_manager.TokenManager.get_valid_token"
BASE = settings.whoop_api_base_url


def _client() -> WhoopClient:
    client = WhoopClient(user_id=1)
    client.token_manager.get_valid_token = AsyncMock(return_value="tok")
    return client


def _cfg(collection: str):
    return next(c for c in WHOOP_CHECK_CONFIGS if c.collection == collection)


@pytest.mark.integration
class TestRangeAssetRun:
    # @spec WHOOP-CAPTURE-001
    @respx.mock
    def test_partition_requests_its_utc_day_in_millisecond_z_format(self, isolate_bronze_root):
        route = respx.get(f"{BASE}/developer/v2/activity/sleep").mock(
            return_value=httpx.Response(200, json={"records": [], "next_token": None})
        )
        with patch(TOKEN_PATCH, new=AsyncMock(return_value="tok")):
            assert materialize([bronze_sleep], partition_key="2025-01-05").success
        params = route.calls.last.request.url.params
        assert params["start"] == "2025-01-05T00:00:00.000Z"
        assert params["end"] == "2025-01-06T00:00:00.000Z"

    # @spec WHOOP-CAPTURE-006
    @respx.mock
    def test_records_count_and_partition_are_materialization_metadata(
        self, isolate_bronze_root
    ):
        respx.get(f"{BASE}/developer/v2/activity/sleep").mock(
            return_value=httpx.Response(
                200, json={"records": [{"id": "a"}, {"id": "b"}], "next_token": None}
            )
        )
        with patch(TOKEN_PATCH, new=AsyncMock(return_value="tok")):
            result = materialize([bronze_sleep], partition_key="2025-01-05")
        (mat,) = result.asset_materializations_for_node("whoop_bronze_sleep")
        assert mat.metadata["records"].value == 2
        assert mat.metadata["partition"].value == "2025-01-05"

    # @spec WHOOP-CAPTURE-007
    @respx.mock
    def test_snapshots_fetch_profile_then_body_into_fetch_date_folder(
        self, isolate_bronze_root, mock_whoop_user_profile, mock_whoop_body_measurement
    ):
        profile = respx.get(f"{BASE}/developer/v2/user/profile/basic").mock(
            return_value=httpx.Response(200, json=mock_whoop_user_profile)
        )
        body = respx.get(f"{BASE}/developer/v2/user/measurement/body").mock(
            return_value=httpx.Response(200, json=mock_whoop_body_measurement)
        )
        today = datetime.now(UTC).strftime("%Y-%m-%d")
        with patch(TOKEN_PATCH, new=AsyncMock(return_value="tok")):
            assert materialize([bronze_snapshots]).success
        calls = [c.request.url.path for c in respx.calls]
        assert calls.index(profile.calls.last.request.url.path) < calls.index(
            body.calls.last.request.url.path
        )
        for collection in ("profile", "body_measurement"):
            assert os.listdir(f"{isolate_bronze_root}/whoop/{collection}") == [f"dt={today}"]


@pytest.mark.unit
@pytest.mark.asyncio
class TestDataRequests:
    # @spec WHOOP-CAPTURE-005
    @respx.mock
    async def test_non_json_2xx_raises_without_retry_or_capture(self, isolate_bronze_root):
        client = _client()
        route = respx.get(f"{BASE}/test").mock(
            return_value=httpx.Response(200, content=b"<html>maintenance</html>")
        )
        with pytest.raises(WhoopAPIError):
            await client._make_request("/test", collection="sleep")
        assert route.call_count == 1
        assert not os.path.exists(isolate_bronze_root)
        await client.aclose()

    # @spec WHOOP-CAPTURE-010
    @respx.mock
    async def test_non_retryable_status_is_not_retried(self):
        client = _client()
        route = respx.get(f"{BASE}/test").mock(return_value=httpx.Response(404, json={}))
        with pytest.raises(WhoopAPIError):
            await client._make_request("/test")
        assert route.call_count == 1
        await client.aclose()

    # @spec WHOOP-CAPTURE-011
    @respx.mock
    async def test_rate_limiter_is_acquired_on_every_attempt(self):
        client = _client()
        client.rate_limiter.acquire = AsyncMock()
        respx.get(f"{BASE}/test").mock(
            side_effect=[
                httpx.Response(429, headers={"Retry-After": "0"}, json={}),
                httpx.Response(200, json={"ok": True}),
            ]
        )
        await client._make_request("/test")
        assert client.rate_limiter.acquire.await_count == 2
        await client.aclose()


@pytest.mark.unit
class TestRetryWait:
    # @spec WHOOP-CAPTURE-009
    @pytest.mark.parametrize(("retry_after", "expected"), [(5.0, 5.0), (120.0, 60.0)])
    def test_retry_after_is_honored_and_capped_at_60s(self, retry_after, expected):
        state = Mock()
        state.outcome.exception.return_value = WhoopRetryableError(
            "busy", status_code=429, retry_after=retry_after
        )
        assert _whoop_retry_wait(state) == expected


@pytest.mark.unit
@pytest.mark.asyncio
class TestPaginationRequests:
    # @spec WHOOP-CAPTURE-PAGE-001
    @respx.mock
    async def test_limit_is_clamped_to_25_and_cursor_is_sent_back(self):
        client = _client()
        route = respx.get(f"{BASE}/developer/v2/activity/sleep").mock(
            side_effect=[
                httpx.Response(200, json={"records": [{"id": "1"}], "next_token": "p2"}),
                httpx.Response(200, json={"records": [{"id": "2"}], "next_token": None}),
            ]
        )
        await client.get_sleep_records(limit=100)
        first, second = (c.request.url.params for c in route.calls)
        assert first["limit"] == "25" and "nextToken" not in first
        assert second["nextToken"] == "p2"
        await client.aclose()


@pytest.mark.unit
class TestCheckWiring:
    # @spec WHOOP-CAPTURE-CHECKS-001
    @pytest.mark.parametrize(
        ("collection", "field"),
        [("sleep", "start"), ("recovery", "created_at"), ("cycle", "start")],
    )
    def test_daily_collections_get_all_four_families(self, collection, field):
        cfg = _cfg(collection)
        assert cfg.enabled_checks == {"freshness", "completeness", "schema", "content"}
        assert (cfg.cadence_hours, cfg.grace_hours, cfg.cadence_days) == (26.0, 6.0, 2)
        assert (cfg.event_date_source, cfg.event_date_field) == ("payload", field)

    # @spec WHOOP-CAPTURE-CHECKS-002
    def test_workout_gets_completeness_at_14_days_and_schema_only(self):
        cfg = _cfg("workout")
        assert cfg.enabled_checks == {"completeness", "schema"}
        assert cfg.cadence_days == 14
        assert (cfg.event_date_source, cfg.event_date_field) == ("payload", "start")

    # @spec WHOOP-CAPTURE-CHECKS-003
    @pytest.mark.parametrize("collection", ["profile", "body_measurement"])
    def test_snapshots_get_freshness_schema_and_content(self, collection):
        cfg = _cfg(collection)
        assert cfg.enabled_checks == {"freshness", "schema", "content"}
        assert (cfg.cadence_hours, cfg.grace_hours) == (26.0, 6.0)

    # @spec WHOOP-CAPTURE-CHECKS-004
    def test_checks_run_hourly_at_minute_0_outside_the_api_pool(self):
        schedule = defs.get_schedule_def("whoop_bronze_checks_hourly")
        assert schedule.cron_schedule == "0 * * * *"
        assert schedule.job_name == "whoop_bronze_checks_job"
        assert whoop_token_health in whoop_checks
        job = defs.resolve_job_def("whoop_bronze_checks_job")
        assert all(getattr(nd, "pool", None) is None for nd in job.graph.node_defs)
