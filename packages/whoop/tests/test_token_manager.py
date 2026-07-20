"""Tests for TokenManager backed by the plaintext-JSON file store."""

import asyncio
from unittest.mock import AsyncMock, Mock

import pytest

from grecohome_core.tokens.file_store import TokenFileStore
from grecohome_whoop.auth.token_manager import TokenManager


@pytest.fixture
def store(tmp_path):
    return TokenFileStore(str(tmp_path / "token.json"))


def _manager(store, oauth_client=None):
    return TokenManager(
        oauth_client=oauth_client or Mock(), refresh_threshold_minutes=5, store=store
    )


@pytest.mark.unit
class TestSaveToken:
    def test_save_token_writes_file(self, store):
        mgr = _manager(store)
        mgr.save_token("acc", "ref", expires_in=3600, scopes=["read:sleep"])
        data = store.read()
        assert data["access_token"] == "acc"
        assert data["refresh_token"] == "ref"
        assert data["scopes"] == ["read:sleep"]
        assert "expires_at" in data


@pytest.mark.unit
@pytest.mark.asyncio
class TestGetValidToken:
    async def test_returns_none_when_no_token(self, store):
        mgr = _manager(store)
        assert await mgr.get_valid_token() is None

    async def test_returns_access_when_not_near_expiry(self, store):
        oauth = Mock()
        oauth.refresh_access_token = AsyncMock()
        mgr = _manager(store, oauth)
        mgr.save_token("good_access", "ref", expires_in=3600)

        assert await mgr.get_valid_token() == "good_access"
        oauth.refresh_access_token.assert_not_awaited()

    async def test_refreshes_when_near_expiry(self, store):
        oauth = Mock()
        oauth.refresh_access_token = AsyncMock(
            return_value={
                "access_token": "new_access",
                "refresh_token": "new_refresh",
                "expires_in": 3600,
                "token_type": "Bearer",
            }
        )
        mgr = _manager(store, oauth)
        mgr.save_token("old_access", "old_refresh", expires_in=60)  # within 5-min threshold

        assert await mgr.get_valid_token() == "new_access"
        oauth.refresh_access_token.assert_awaited_once_with("old_refresh")
        # File now holds the rotated tokens and a far-future expiry.
        data = store.read()
        assert data["access_token"] == "new_access"
        assert data["refresh_token"] == "new_refresh"
        assert await mgr.is_token_valid() is True

    async def test_refresh_rotation_fallback_keeps_old_refresh(self, store):
        oauth = Mock()
        oauth.refresh_access_token = AsyncMock(
            return_value={"access_token": "new_access", "expires_in": 3600}  # no refresh_token
        )
        mgr = _manager(store, oauth)
        mgr.save_token("old_access", "old_refresh", expires_in=60)

        assert await mgr.get_valid_token() == "new_access"
        assert store.read()["refresh_token"] == "old_refresh"  # preserved

    async def test_missing_rotation_logs_warning(self, store):
        # A 200 without a rotated refresh token must not pass silently — it's how
        # a refresh failure becomes a silent outage.
        from structlog.testing import capture_logs

        oauth = Mock()
        oauth.refresh_access_token = AsyncMock(
            return_value={"access_token": "new_access", "expires_in": 3600}  # no refresh_token
        )
        mgr = _manager(store, oauth)
        mgr.save_token("old_access", "old_refresh", expires_in=60)

        with capture_logs() as logs:
            await mgr.get_valid_token()
        assert any(e.get("event") == "whoop_token_no_rotation" for e in logs)

    async def test_concurrent_callers_refresh_only_once(self, store):
        # The double-spend guard: many callers hitting a near-expiry token must
        # trigger exactly one refresh (a second replay of the consumed rotating
        # refresh token is what gets the Whoop grant revoked). The losers reuse
        # the token the winner just rotated.
        oauth = Mock()

        async def slow_refresh(refresh_token):
            await asyncio.sleep(0.05)  # widen the race window
            return {
                "access_token": "new_access",
                "refresh_token": "new_refresh",
                "expires_in": 3600,
                "token_type": "Bearer",
            }

        oauth.refresh_access_token = AsyncMock(side_effect=slow_refresh)
        mgr = _manager(store, oauth)
        mgr.save_token("old_access", "old_refresh", expires_in=60)  # near expiry

        results = await asyncio.gather(*[mgr.get_valid_token() for _ in range(5)])

        assert results == ["new_access"] * 5
        oauth.refresh_access_token.assert_awaited_once_with("old_refresh")

    async def test_malformed_refresh_raises_and_leaves_file_unchanged(self, store):
        oauth = Mock()
        oauth.refresh_access_token = AsyncMock(return_value={"unexpected": "shape"})
        mgr = _manager(store, oauth)
        mgr.save_token("old_access", "old_refresh", expires_in=60)

        with pytest.raises(ValueError, match="missing access_token/expires_in"):
            await mgr.get_valid_token()
        # The stored token is untouched by the failed refresh.
        assert store.read()["access_token"] == "old_access"


@pytest.mark.unit
@pytest.mark.asyncio
class TestIsTokenValid:
    async def test_false_when_no_token(self, store):
        assert await _manager(store).is_token_valid() is False

    async def test_true_for_unexpired(self, store):
        mgr = _manager(store)
        mgr.save_token("a", "r", expires_in=3600)
        assert await mgr.is_token_valid() is True

    async def test_false_for_expired(self, store):
        mgr = _manager(store)
        mgr.save_token("a", "r", expires_in=-10)  # already expired
        assert await mgr.is_token_valid() is False
