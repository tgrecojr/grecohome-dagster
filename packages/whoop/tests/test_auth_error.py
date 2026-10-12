"""Tests for WhoopAuthError: a token failure must never be retried by the API client.

The API client fetches its bearer token inside its data-request retry, which
retries timeouts and network errors. A refresh that timed out after Whoop rotated
the refresh token must not be re-run by that retry -- it would replay the consumed
token and revoke the grant.
"""

from unittest.mock import AsyncMock, Mock, patch

import httpx
import pytest

from grecohome_whoop.api.whoop_client import (
    WhoopAPIError,
    WhoopAuthError,
    WhoopClient,
    WhoopRetryableError,
)


def _client_with_token(**token_mock_kwargs) -> tuple[WhoopClient, AsyncMock]:
    client = WhoopClient(user_id=1)
    get_token = AsyncMock(**token_mock_kwargs)
    client.token_manager.get_valid_token = get_token
    return client, get_token


@pytest.mark.unit
def test_auth_error_is_api_error_but_never_retryable():
    assert issubclass(WhoopAuthError, WhoopAPIError)
    assert not issubclass(WhoopAuthError, WhoopRetryableError)


@pytest.mark.unit
@pytest.mark.asyncio
class TestTokenFailureIsNotRetried:
    # @spec WHOOP-AUTH-009
    @pytest.mark.parametrize(
        "exc",
        [httpx.ReadTimeout("slow"), httpx.ConnectError("down"), ValueError("bad file")],
    )
    async def test_token_failure_raises_auth_error_without_retry(self, exc):
        client, get_token = _client_with_token(side_effect=exc)
        with pytest.raises(WhoopAuthError):
            await client._make_request("/developer/v2/activity/sleep")
        get_token.assert_awaited_once()

    # @spec WHOOP-AUTH-010
    async def test_missing_token_raises_auth_error_without_retry(self):
        client, get_token = _client_with_token(return_value=None)
        with pytest.raises(WhoopAuthError, match="No valid access token"):
            await client._make_request("/developer/v2/activity/sleep")
        get_token.assert_awaited_once()


@pytest.mark.unit
@pytest.mark.asyncio
class TestReRaisedMessagesCarryTypeOnly:
    # @spec WHOOP-LOG-002
    async def test_auth_error_message_has_type_name_not_text(self):
        original = RuntimeError("Bearer leaked-token-value")
        client, _ = _client_with_token(side_effect=original)
        with pytest.raises(WhoopAuthError) as info:
            await client._make_request("/developer/v2/activity/sleep")
        assert "RuntimeError" in str(info.value)
        assert "leaked-token-value" not in str(info.value)
        assert info.value.__cause__ is original  # chain kept (accepted trade-off)

    async def test_api_error_message_has_type_name_not_text(self):
        client, _ = _client_with_token(return_value="tok")
        http = Mock()
        http.get = AsyncMock(side_effect=RuntimeError("Illegal header value b'Bearer leaked'"))
        with patch.object(client, "_get_client", return_value=http):
            with pytest.raises(WhoopAPIError) as info:
                await client._make_request("/developer/v2/activity/sleep")
        assert "RuntimeError" in str(info.value)
        assert "leaked" not in str(info.value)
