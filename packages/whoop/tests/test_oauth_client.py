"""Tests for the Whoop OAuth client."""

from urllib.parse import parse_qs, urlparse

import httpx
import pytest
import respx

from grecohome_whoop.auth.oauth_client import WhoopOAuthClient


@pytest.mark.unit
class TestWhoopOAuthClient:
    def test_initialization(self):
        client = WhoopOAuthClient()
        assert client.client_id
        assert client.client_secret
        assert client.redirect_uri
        assert client.auth_url
        assert client.token_url
        for scope in (
            "read:sleep", "read:workout", "read:recovery", "read:cycles",
            "read:profile", "read:body_measurement", "offline",
        ):
            assert scope in client.scopes

    def test_generate_pkce_pair(self):
        client = WhoopOAuthClient()
        verifier, challenge = client.generate_pkce_pair()
        assert len(verifier) >= 43
        assert challenge and verifier != challenge

    def test_generate_pkce_pair_unique(self):
        client = WhoopOAuthClient()
        v1, c1 = client.generate_pkce_pair()
        v2, c2 = client.generate_pkce_pair()
        assert v1 != v2 and c1 != c2

    def test_get_authorization_url(self):
        client = WhoopOAuthClient()
        auth_url, state, verifier = client.get_authorization_url()
        params = parse_qs(urlparse(auth_url).query)
        assert client.auth_url in auth_url
        assert params["client_id"][0] == client.client_id
        assert params["redirect_uri"][0] == client.redirect_uri
        assert params["response_type"][0] == "code"
        assert params["state"][0] == state
        assert "code_challenge" in params
        assert params["code_challenge_method"][0] == "S256"
        assert state and verifier

    def test_get_authorization_url_with_custom_state(self):
        client = WhoopOAuthClient()
        auth_url, state, _ = client.get_authorization_url(state="my_state")
        assert parse_qs(urlparse(auth_url).query)["state"][0] == "my_state"
        assert state == "my_state"

    def test_authorization_url_includes_scopes(self):
        client = WhoopOAuthClient()
        auth_url, _, _ = client.get_authorization_url()
        scopes = parse_qs(urlparse(auth_url).query)["scope"][0].split(" ")
        for scope in (
            "read:sleep", "read:workout", "read:recovery", "read:cycles",
            "read:profile", "read:body_measurement", "offline",
        ):
            assert scope in scopes


@pytest.mark.unit
@pytest.mark.asyncio
class TestWhoopOAuthClientAsync:
    @respx.mock
    async def test_exchange_code_for_token_success(self):
        client = WhoopOAuthClient()
        respx.post(client.token_url).mock(
            return_value=httpx.Response(
                200,
                json={
                    "access_token": "test_access_token",
                    "refresh_token": "test_refresh_token",
                    "token_type": "Bearer",
                    "expires_in": 3600,
                    "scope": "read:sleep read:workout",
                },
            )
        )
        token_data = await client.exchange_code_for_token("test_code", "test_verifier")
        assert token_data["access_token"] == "test_access_token"
        assert token_data["refresh_token"] == "test_refresh_token"
        assert token_data["expires_in"] == 3600

    @respx.mock
    async def test_exchange_code_for_token_failure(self):
        client = WhoopOAuthClient()
        respx.post(client.token_url).mock(
            return_value=httpx.Response(400, json={"error": "invalid_grant"})
        )
        with pytest.raises(httpx.HTTPStatusError):
            await client.exchange_code_for_token("invalid_code", "test_verifier")

    @respx.mock
    async def test_refresh_access_token_success(self):
        client = WhoopOAuthClient()
        respx.post(client.token_url).mock(
            return_value=httpx.Response(
                200,
                json={
                    "access_token": "new_access_token",
                    "refresh_token": "new_refresh_token",
                    "token_type": "Bearer",
                    "expires_in": 3600,
                },
            )
        )
        token_data = await client.refresh_access_token("old_refresh_token")
        assert token_data["access_token"] == "new_access_token"
        assert token_data["refresh_token"] == "new_refresh_token"

    @respx.mock
    async def test_refresh_access_token_failure(self):
        client = WhoopOAuthClient()
        respx.post(client.token_url).mock(
            return_value=httpx.Response(400, json={"error": "invalid_token"})
        )
        with pytest.raises(httpx.HTTPStatusError):
            await client.refresh_access_token("invalid_refresh_token")

    @respx.mock
    async def test_refresh_does_not_retry_on_400(self):
        # 400 invalid_grant is terminal: the refresh token is dead, so retrying
        # would only hammer Whoop with a known-bad grant. Must be a single call.
        client = WhoopOAuthClient()
        respx.post(client.token_url).mock(
            return_value=httpx.Response(400, json={"error": "invalid_grant"})
        )
        with pytest.raises(httpx.HTTPStatusError):
            await client.refresh_access_token("dead_refresh_token")
        assert respx.calls.call_count == 1

    @respx.mock
    async def test_refresh_does_not_retry_on_503(self):
        # A 5xx means Whoop *received* the refresh POST and may have registered the
        # single-use token; retrying replays it and trips reuse-detection, revoking
        # the grant (2026-06-11 503 storm, 2026-07-22 502). Must be a single call.
        client = WhoopOAuthClient()
        respx.post(client.token_url).mock(
            return_value=httpx.Response(503, json={})
        )
        with pytest.raises(httpx.HTTPStatusError):
            await client.refresh_access_token("old_refresh_token")
        assert respx.calls.call_count == 1

    @respx.mock
    async def test_refresh_does_not_retry_on_502(self):
        # The 2026-07-22 outage: a 502 during rotation. Single attempt, re-raise.
        client = WhoopOAuthClient()
        respx.post(client.token_url).mock(
            return_value=httpx.Response(502, json={})
        )
        with pytest.raises(httpx.HTTPStatusError):
            await client.refresh_access_token("old_refresh_token")
        assert respx.calls.call_count == 1

    @respx.mock
    async def test_exchange_code_network_error(self):
        client = WhoopOAuthClient()
        respx.post(client.token_url).mock(side_effect=httpx.NetworkError)
        with pytest.raises(httpx.NetworkError):
            await client.exchange_code_for_token("test_code", "test_verifier")

    @respx.mock
    async def test_refresh_does_not_retry_on_read_timeout(self):
        # A ReadTimeout means the POST reached Whoop -- which rotates the single-use
        # refresh token non-atomically, so it may already be consumed. Retrying would
        # replay the consumed token and revoke the grant (the 2026-07-20 outage).
        # Must be a single call, then re-raise.
        client = WhoopOAuthClient()
        respx.post(client.token_url).mock(side_effect=httpx.ReadTimeout("slow"))
        with pytest.raises(httpx.ReadTimeout):
            await client.refresh_access_token("old_refresh_token")
        assert respx.calls.call_count == 1

    @respx.mock
    async def test_refresh_retries_on_connect_error_then_succeeds(self):
        # A ConnectError happens before the request lands, so no rotation could have
        # occurred -- safe to retry.
        client = WhoopOAuthClient()
        route = respx.post(client.token_url)
        route.side_effect = [
            httpx.ConnectError("no route"),
            httpx.Response(
                200,
                json={
                    "access_token": "new_access_token",
                    "refresh_token": "new_refresh_token",
                    "expires_in": 3600,
                },
            ),
        ]
        token_data = await client.refresh_access_token("old_refresh_token")
        assert token_data["access_token"] == "new_access_token"
        assert route.call_count == 2
