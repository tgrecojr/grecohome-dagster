"""Tests that the Whoop token and API paths never write tokens or secrets to logs."""

from unittest.mock import AsyncMock, Mock, patch

import httpx
import pytest
import respx
from structlog.testing import capture_logs

from grecohome_whoop.api.whoop_client import WhoopAPIError, WhoopClient
from grecohome_whoop.auth.oauth_client import WhoopOAuthClient

LEAKY_TEXT = "leaky-value-7f3a9c"


def _all_values(logs: list[dict]) -> str:
    return " ".join(f"{k}={v}" for entry in logs for k, v in entry.items())


def _assert_type_only(logs: list[dict], event: str) -> None:
    entries = [e for e in logs if e["event"] == event]
    assert entries, f"expected a {event!r} log entry"
    for entry in entries:
        assert entry.get("error_type") == "RuntimeError"
        assert "exc_info" not in entry
    assert LEAKY_TEXT not in _all_values(logs)


@pytest.mark.unit
@pytest.mark.asyncio
class TestUnexpectedFailuresLogTypeOnly:
    # @spec WHOOP-AUTH-LOG-001
    @respx.mock
    async def test_refresh_unexpected_failure(self):
        client = WhoopOAuthClient()
        respx.post(client.token_url).mock(side_effect=RuntimeError(LEAKY_TEXT))
        with capture_logs() as logs, pytest.raises(RuntimeError):
            await client.refresh_access_token("r1")
        _assert_type_only(logs, "Unexpected error during token refresh")

    @respx.mock
    async def test_code_exchange_unexpected_failure(self):
        client = WhoopOAuthClient()
        respx.post(client.token_url).mock(side_effect=RuntimeError(LEAKY_TEXT))
        with capture_logs() as logs, pytest.raises(RuntimeError):
            await client.exchange_code_for_token("code", "verifier")
        _assert_type_only(logs, "Unexpected error during token exchange")

    async def test_api_request_unexpected_failure(self):
        client = WhoopClient(user_id=1)
        client.token_manager.get_valid_token = AsyncMock(return_value="tok")
        http = Mock()
        http.get = AsyncMock(side_effect=RuntimeError(LEAKY_TEXT))
        with patch.object(client, "_get_client", return_value=http):
            with capture_logs() as logs, pytest.raises(WhoopAPIError):
                await client._make_request("/developer/v2/activity/sleep")
        _assert_type_only(logs, "Unexpected error during API request")


@pytest.mark.unit
@pytest.mark.asyncio
class TestNoTokenFragmentsLogged:
    # @spec WHOOP-AUTH-LOG-003
    @respx.mock
    async def test_exchange_and_refresh_log_no_part_of_any_token(self):
        client = WhoopOAuthClient()
        respx.post(client.token_url).mock(
            return_value=httpx.Response(
                200,
                json={
                    "access_token": "ACCESSxyz123456789",
                    "refresh_token": "REFRESHxyz123456789",
                    "expires_in": 3600,
                },
            )
        )
        with capture_logs() as logs:
            await client.exchange_code_for_token("AUTHCODExyz123456789", "VERIFIERxyz12345")
            await client.refresh_access_token("OLDREFRESHxyz12345")
        logged = _all_values(logs)
        for fragment in (
            "AUTHCODE", "VERIFIER", "ACCESSxyz", "REFRESHxyz", "OLDREFRESH",
            client.client_secret[:6],
        ):
            assert fragment not in logged, f"a fragment of {fragment!r} was logged"
