"""Tests for the Whoop token-safety guards.

Whoop's refresh token is single-use: presenting a consumed one revokes the whole
grant. These tests pin the guards that keep a refresh token from ever being
presented twice.
"""

import asyncio
import threading
from datetime import UTC, datetime
from unittest.mock import AsyncMock, Mock, patch

import httpx
import pytest
import respx
from structlog.testing import capture_logs
from tenacity import wait_none

from grecohome_core.tokens.file_lock import InterProcessLock
from grecohome_core.tokens.file_store import TokenFileStore
from grecohome_whoop.auth.oauth_client import WhoopOAuthClient
from grecohome_whoop.auth.token_manager import TokenManager


@pytest.fixture
def store(tmp_path):
    return TokenFileStore(str(tmp_path / "token.json"))


@pytest.fixture
def no_retry_wait(monkeypatch):
    """Skip the refresh's exponential backoff so retry tests run instantly."""
    monkeypatch.setattr(WhoopOAuthClient.refresh_access_token.retry, "wait", wait_none())


def _manager(store, oauth_client=None):
    return TokenManager(
        oauth_client=oauth_client or Mock(), refresh_threshold_minutes=5, store=store
    )


def _ok(**extra):
    return {"access_token": "new_access", "expires_in": 3600, **extra}


@pytest.mark.unit
@pytest.mark.asyncio
class TestRefreshSerialization:
    # @spec WHOOP-AUTH-003
    async def test_refresh_waits_for_host_wide_file_lock(self, store):
        oauth = Mock()
        oauth.refresh_access_token = AsyncMock(return_value=_ok(refresh_token="r2"))
        mgr = _manager(store, oauth)
        mgr.save_token("old_access", "r1", expires_in=60)  # near expiry

        holder = InterProcessLock(f"{store.path}.lock")  # another process's refresh
        holder.acquire()
        try:
            task = asyncio.create_task(mgr.get_valid_token())
            await asyncio.sleep(0.2)
            assert not task.done()
            oauth.refresh_access_token.assert_not_awaited()
        finally:
            holder.release()
        assert await task == "new_access"
        oauth.refresh_access_token.assert_awaited_once_with("r1")


@pytest.mark.unit
@pytest.mark.asyncio
class TestRefreshRequest:
    # @spec WHOOP-AUTH-006
    @respx.mock
    async def test_token_endpoint_requests_use_30s_timeout_5s_connect(self):
        client = WhoopOAuthClient()
        respx.post(client.token_url).mock(
            return_value=httpx.Response(200, json=_ok(refresh_token="r2"))
        )
        with patch(
            "grecohome_whoop.auth.oauth_client.httpx.AsyncClient", wraps=httpx.AsyncClient
        ) as ac:
            await client.refresh_access_token("r1")
            await client.exchange_code_for_token("code", "verifier")
        for call in ac.call_args_list:
            timeout = call.kwargs["timeout"]
            assert (timeout.read, timeout.connect) == (30.0, 5.0)
        assert ac.call_count == 2

    # @spec WHOOP-AUTH-007
    @respx.mock
    async def test_connect_errors_get_at_most_three_attempts(self, no_retry_wait):
        client = WhoopOAuthClient()
        route = respx.post(client.token_url).mock(side_effect=httpx.ConnectError("down"))
        with pytest.raises(httpx.ConnectError):
            await client.refresh_access_token("r1")
        assert route.call_count == 3

    # @spec WHOOP-AUTH-015
    @respx.mock
    async def test_400_logs_invalid_grant_at_error(self):
        client = WhoopOAuthClient()
        respx.post(client.token_url).mock(
            return_value=httpx.Response(400, json={"error": "invalid_grant"})
        )
        with capture_logs() as logs, pytest.raises(httpx.HTTPStatusError):
            await client.refresh_access_token("r1")
        events = [e for e in logs if e["event"] == "whoop_token_invalid_grant"]
        assert events and events[0]["log_level"] == "error"
        assert events[0]["status_code"] == 400


@pytest.mark.unit
@pytest.mark.asyncio
class TestMalformedRefreshResponse:
    # @spec WHOOP-AUTH-013
    async def test_rotated_refresh_token_is_kept_when_response_is_malformed(self, store):
        oauth = Mock()
        oauth.refresh_access_token = AsyncMock(
            side_effect=[{"refresh_token": "r2"}, _ok(refresh_token="r3")]
        )
        mgr = _manager(store, oauth)
        mgr.save_token("old_access", "r1", expires_in=60)

        with pytest.raises(ValueError, match="missing access_token/expires_in"):
            await mgr.get_valid_token()
        data = store.read()
        assert data["refresh_token"] == "r2"  # R' saved; R (r1) is consumed
        assert datetime.fromisoformat(data["expires_at"]) < datetime.now(UTC)

        # The next caller refreshes with R', never replaying the consumed R.
        assert await mgr.get_valid_token() == "new_access"
        assert oauth.refresh_access_token.await_args_list[-1].args == ("r2",)


@pytest.mark.unit
class TestSaveTokenLock:
    # @spec WHOOP-AUTH-016
    def test_reauth_write_waits_for_host_wide_file_lock(self, store):
        mgr = _manager(store)
        holder = InterProcessLock(f"{store.path}.lock")  # an in-flight refresh
        holder.acquire()
        writer = threading.Thread(target=mgr.save_token, args=("fresh_access", "fresh", 3600))
        try:
            writer.start()
            writer.join(timeout=0.3)
            assert writer.is_alive(), "re-auth wrote the token without taking the lock"
        finally:
            holder.release()
        writer.join(timeout=5)
        assert store.read()["refresh_token"] == "fresh"
