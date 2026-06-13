"""Tests for the Whoop API client."""

import json
import os
from unittest.mock import AsyncMock, patch

import httpx
import pytest
import respx

from grecohome_core.http.rate_limiter import RateLimiter
from grecohome_whoop.api.whoop_client import WhoopAPIError, WhoopClient
from grecohome_whoop.auth.token_manager import TokenManager


def _bronze_files(root: str) -> list[str]:
    return [os.path.join(d, n) for d, _s, names in os.walk(root) for n in names]


@pytest.mark.unit
class TestWhoopClient:
    def test_initialization(self):
        client = WhoopClient(user_id=1)
        assert client.user_id == 1
        assert client.token_manager is not None
        assert client.rate_limiter is not None
        assert client.timeout == 30.0

    def test_custom_components(self):
        token_manager = TokenManager()
        rate_limiter = RateLimiter()
        client = WhoopClient(
            user_id=1, token_manager=token_manager, rate_limiter=rate_limiter, timeout=60.0
        )
        assert client.token_manager is token_manager
        assert client.rate_limiter is rate_limiter
        assert client.timeout == 60.0


@pytest.mark.unit
@pytest.mark.asyncio
class TestWhoopClientAsync:
    async def test_get_headers_success(self):
        client = WhoopClient(user_id=1)
        with patch.object(
            client.token_manager, "get_valid_token", new=AsyncMock(return_value="tok123")
        ):
            headers = await client._get_headers()
        assert headers["Authorization"] == "Bearer tok123"
        assert headers["Accept"] == "application/json"

    async def test_get_headers_no_token(self):
        client = WhoopClient(user_id=1)
        with patch.object(
            client.token_manager, "get_valid_token", new=AsyncMock(return_value=None)
        ):
            with pytest.raises(WhoopAPIError, match="No valid access token"):
                await client._get_headers()

    @respx.mock
    async def test_make_request_success(self):
        client = WhoopClient(user_id=1)
        with patch.object(
            client.token_manager, "get_valid_token", new=AsyncMock(return_value="tok")
        ), patch.object(client.rate_limiter, "acquire", new=AsyncMock()):
            respx.get(f"{client.base_url}/test").mock(
                return_value=httpx.Response(200, json={"data": "test"})
            )
            assert await client._make_request("/test") == {"data": "test"}
        await client.aclose()

    @respx.mock
    async def test_make_request_http_error(self):
        client = WhoopClient(user_id=1)
        with patch.object(
            client.token_manager, "get_valid_token", new=AsyncMock(return_value="tok")
        ), patch.object(client.rate_limiter, "acquire", new=AsyncMock()):
            respx.get(f"{client.base_url}/test").mock(
                return_value=httpx.Response(404, json={"error": "Not found"})
            )
            with pytest.raises(WhoopAPIError):
                await client._make_request("/test")
        await client.aclose()

    @respx.mock
    async def test_make_request_error_message_excludes_body(self):
        client = WhoopClient(user_id=1)
        with patch.object(
            client.token_manager, "get_valid_token", new=AsyncMock(return_value="tok")
        ), patch.object(client.rate_limiter, "acquire", new=AsyncMock()):
            respx.get(f"{client.base_url}/test").mock(
                return_value=httpx.Response(404, json={"secret_detail": "do-not-log-me"})
            )
            with pytest.raises(WhoopAPIError) as exc_info:
                await client._make_request("/test")
            assert "do-not-log-me" not in str(exc_info.value)
            assert exc_info.value.status_code == 404
        await client.aclose()

    @respx.mock
    async def test_make_request_retries_on_429(self):
        client = WhoopClient(user_id=1)
        with patch.object(
            client.token_manager, "get_valid_token", new=AsyncMock(return_value="tok")
        ), patch.object(client.rate_limiter, "acquire", new=AsyncMock()):
            route = respx.get(f"{client.base_url}/test")
            route.side_effect = [
                httpx.Response(429, headers={"Retry-After": "0"}, json={}),
                httpx.Response(200, json={"ok": True}),
            ]
            assert await client._make_request("/test") == {"ok": True}
            assert route.call_count == 2
        await client.aclose()

    @respx.mock
    async def test_make_request_gives_up_after_retries(self):
        client = WhoopClient(user_id=1)
        with patch.object(
            client.token_manager, "get_valid_token", new=AsyncMock(return_value="tok")
        ), patch.object(client.rate_limiter, "acquire", new=AsyncMock()):
            respx.get(f"{client.base_url}/test").mock(
                return_value=httpx.Response(503, headers={"Retry-After": "0"}, json={})
            )
            with pytest.raises(WhoopAPIError):
                await client._make_request("/test")
            assert respx.calls.call_count == 3  # stop_after_attempt(3)
        await client.aclose()

    async def test_http_client_is_reused_and_closeable(self):
        client = WhoopClient(user_id=1)
        c1 = client._get_client()
        assert client._get_client() is c1
        assert not c1.is_closed
        await client.aclose()
        assert c1.is_closed
        assert client._get_client() is not c1
        await client.aclose()

    @respx.mock
    async def test_get_sleep_records(self, mock_whoop_sleep_response):
        client = WhoopClient(user_id=1)
        with patch.object(
            client.token_manager, "get_valid_token", new=AsyncMock(return_value="tok")
        ), patch.object(client.rate_limiter, "acquire", new=AsyncMock()):
            respx.get(f"{client.base_url}/developer/v2/activity/sleep").mock(
                return_value=httpx.Response(200, json=mock_whoop_sleep_response)
            )
            records = await client.get_sleep_records()
        assert len(records) == 1 and "id" in records[0]
        await client.aclose()

    @respx.mock
    async def test_get_workout_records(self, mock_whoop_workout_response):
        client = WhoopClient(user_id=1)
        with patch.object(
            client.token_manager, "get_valid_token", new=AsyncMock(return_value="tok")
        ), patch.object(client.rate_limiter, "acquire", new=AsyncMock()):
            respx.get(f"{client.base_url}/developer/v2/activity/workout").mock(
                return_value=httpx.Response(200, json=mock_whoop_workout_response)
            )
            records = await client.get_workout_records()
        assert records[0]["sport_name"] == "Running"
        await client.aclose()

    @respx.mock
    async def test_get_recovery_records(self, mock_whoop_recovery_response):
        client = WhoopClient(user_id=1)
        with patch.object(
            client.token_manager, "get_valid_token", new=AsyncMock(return_value="tok")
        ), patch.object(client.rate_limiter, "acquire", new=AsyncMock()):
            respx.get(f"{client.base_url}/developer/v2/recovery").mock(
                return_value=httpx.Response(200, json=mock_whoop_recovery_response)
            )
            records = await client.get_recovery_records()
        assert "recovery_score" in records[0]["score"]
        await client.aclose()

    @respx.mock
    async def test_get_cycle_records(self, mock_whoop_cycle_response):
        client = WhoopClient(user_id=1)
        with patch.object(
            client.token_manager, "get_valid_token", new=AsyncMock(return_value="tok")
        ), patch.object(client.rate_limiter, "acquire", new=AsyncMock()):
            respx.get(f"{client.base_url}/developer/v2/cycle").mock(
                return_value=httpx.Response(200, json=mock_whoop_cycle_response)
            )
            records = await client.get_cycle_records()
        assert len(records) == 1
        await client.aclose()

    @respx.mock
    async def test_get_user_profile(self, mock_whoop_user_profile):
        client = WhoopClient(user_id=1)
        with patch.object(
            client.token_manager, "get_valid_token", new=AsyncMock(return_value="tok")
        ), patch.object(client.rate_limiter, "acquire", new=AsyncMock()):
            respx.get(f"{client.base_url}/developer/v2/user/profile/basic").mock(
                return_value=httpx.Response(200, json=mock_whoop_user_profile)
            )
            profile = await client.get_user_profile()
        assert profile["first_name"] == "Test"
        await client.aclose()

    @respx.mock
    async def test_pagination(self):
        client = WhoopClient(user_id=1)
        with patch.object(
            client.token_manager, "get_valid_token", new=AsyncMock(return_value="tok")
        ), patch.object(client.rate_limiter, "acquire", new=AsyncMock()):
            route = respx.get(f"{client.base_url}/developer/v2/activity/sleep")
            route.side_effect = [
                httpx.Response(200, json={"records": [{"id": "1"}], "next_token": "p2"}),
                httpx.Response(200, json={"records": [{"id": "2"}], "next_token": None}),
            ]
            records = await client.get_sleep_records()
        assert [r["id"] for r in records] == ["1", "2"]
        await client.aclose()

    @respx.mock
    async def test_request_with_date_range(self, date_range):
        client = WhoopClient(user_id=1)
        start, end = date_range
        with patch.object(
            client.token_manager, "get_valid_token", new=AsyncMock(return_value="tok")
        ), patch.object(client.rate_limiter, "acquire", new=AsyncMock()):
            route = respx.get(f"{client.base_url}/developer/v2/activity/sleep")
            route.mock(return_value=httpx.Response(200, json={"records": [], "next_token": None}))
            await client.get_sleep_records(start=start, end=end)
            url = str(route.calls.last.request.url)
        assert "start" in url and "end" in url
        await client.aclose()


@pytest.mark.unit
@pytest.mark.asyncio
class TestWhoopClientBronzeCapture:
    """Capture is always on; verify it fires correctly through the client."""

    @respx.mock
    async def test_successful_fetch_captures_payload(
        self, isolate_bronze_root, mock_whoop_recovery_response
    ):
        client = WhoopClient(user_id=1)
        with patch.object(
            client.token_manager, "get_valid_token", new=AsyncMock(return_value="tok")
        ), patch.object(client.rate_limiter, "acquire", new=AsyncMock()):
            respx.get(f"{client.base_url}/developer/v2/recovery").mock(
                return_value=httpx.Response(200, json=mock_whoop_recovery_response)
            )
            await client.get_recovery_records()
        payloads = [
            f for f in _bronze_files(isolate_bronze_root)
            if f.endswith(".json") and not f.endswith(".meta.json")
        ]
        assert len(payloads) == 1
        with open(payloads[0]) as fh:
            assert json.load(fh) == mock_whoop_recovery_response
        await client.aclose()

    @respx.mock
    async def test_empty_terminator_page_is_not_captured(self, isolate_bronze_root):
        client = WhoopClient(user_id=1)
        with patch.object(
            client.token_manager, "get_valid_token", new=AsyncMock(return_value="tok")
        ), patch.object(client.rate_limiter, "acquire", new=AsyncMock()):
            respx.get(f"{client.base_url}/developer/v2/recovery").mock(
                return_value=httpx.Response(200, json={"records": [], "next_token": None})
            )
            records = await client.get_recovery_records()
        assert records == []
        assert _bronze_files(isolate_bronze_root) == []
        await client.aclose()

    @respx.mock
    async def test_error_body_is_not_captured(self, isolate_bronze_root):
        """An error response still raises, but its body never lands in bronze.

        Persisting an error envelope (e.g. a 401 ``Authorization was not valid``)
        would poison the profile content/integrity check, so non-2xx responses are
        dropped -- only the status is logged.
        """
        client = WhoopClient(user_id=1)
        with patch.object(
            client.token_manager, "get_valid_token", new=AsyncMock(return_value="tok")
        ), patch.object(client.rate_limiter, "acquire", new=AsyncMock()):
            respx.get(f"{client.base_url}/developer/v2/user/measurement/body").mock(
                return_value=httpx.Response(403, json={"error": "missing scope"})
            )
            with pytest.raises(WhoopAPIError):
                await client.get_body_measurement()
        assert _bronze_files(isolate_bronze_root) == []
        await client.aclose()

    @respx.mock
    async def test_bronze_dt_routes_capture_to_partition_folder(
        self, isolate_bronze_root, mock_whoop_sleep_response
    ):
        client = WhoopClient(user_id=1, bronze_dt="2025-01-05")
        with patch.object(
            client.token_manager, "get_valid_token", new=AsyncMock(return_value="tok")
        ), patch.object(client.rate_limiter, "acquire", new=AsyncMock()):
            respx.get(f"{client.base_url}/developer/v2/activity/sleep").mock(
                return_value=httpx.Response(200, json=mock_whoop_sleep_response)
            )
            await client.get_sleep_records()
        files = _bronze_files(isolate_bronze_root)
        assert files and all("dt=2025-01-05" in f for f in files)
        await client.aclose()

    @respx.mock
    async def test_paginate_stops_on_nonadvancing_cursor(self):
        """A next_token that never advances must terminate, not loop forever."""
        client = WhoopClient(user_id=1)
        with patch.object(
            client.token_manager, "get_valid_token", new=AsyncMock(return_value="tok")
        ), patch.object(client.rate_limiter, "acquire", new=AsyncMock()):
            respx.get(f"{client.base_url}/test").mock(
                return_value=httpx.Response(
                    200, json={"records": [{"id": 1}], "next_token": "STUCK"}
                )
            )
            records = [r async for r in client._paginate("/test")]
        # page 1 records the token; page 2 sees it repeat -> stop. Bounded, not infinite.
        assert len(records) == 2
        assert respx.calls.call_count == 2
        await client.aclose()

    @respx.mock
    async def test_paginate_stops_at_max_pages(self):
        """An advancing-but-never-clearing cursor is bounded by max_pages."""
        client = WhoopClient(user_id=1)
        counter = {"n": 0}

        def _responder(request):
            counter["n"] += 1
            return httpx.Response(
                200, json={"records": [{"id": counter["n"]}], "next_token": f"t{counter['n']}"}
            )

        with patch.object(
            client.token_manager, "get_valid_token", new=AsyncMock(return_value="tok")
        ), patch.object(client.rate_limiter, "acquire", new=AsyncMock()):
            respx.get(f"{client.base_url}/test").mock(side_effect=_responder)
            records = [r async for r in client._paginate("/test", max_pages=3)]
        assert len(records) == 3
        assert respx.calls.call_count == 3
        await client.aclose()

    @respx.mock
    async def test_paginate_normal_termination(self):
        """Normal pagination still ends when next_token clears (guards don't interfere)."""
        client = WhoopClient(user_id=1)
        pages = [
            httpx.Response(200, json={"records": [{"id": 1}], "next_token": "t1"}),
            httpx.Response(200, json={"records": [{"id": 2}]}),  # no next_token -> done
        ]
        with patch.object(
            client.token_manager, "get_valid_token", new=AsyncMock(return_value="tok")
        ), patch.object(client.rate_limiter, "acquire", new=AsyncMock()):
            respx.get(f"{client.base_url}/test").mock(side_effect=pages)
            records = [r async for r in client._paginate("/test")]
        assert [r["id"] for r in records] == [1, 2]
        assert respx.calls.call_count == 2
        await client.aclose()
