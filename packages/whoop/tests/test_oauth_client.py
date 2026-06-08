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
    async def test_exchange_code_network_error(self):
        client = WhoopOAuthClient()
        respx.post(client.token_url).mock(side_effect=httpx.NetworkError)
        with pytest.raises(httpx.NetworkError):
            await client.exchange_code_for_token("test_code", "test_verifier")
