"""Tests for garmin login retry behaviour.

The transient Garmin SSO failure ("JWT_WEB cookie not set after ticket
consumption") surfaces during Dagster *resource init*, before any op runs, so an
op-level RetryPolicy cannot catch it. ``login`` wraps the whole construct+login
cycle in a tenacity retry; these tests pin that behaviour.
"""

import pytest
from garminconnect.exceptions import (
    GarminConnectAuthenticationError,
    GarminConnectConnectionError,
)
from grecohome_garmin import auth
from grecohome_garmin.config import GarminSettings


@pytest.fixture
def settings():
    return GarminSettings()


@pytest.fixture(autouse=True)
def no_backoff_sleep(monkeypatch):
    """Strip tenacity's wait so retries don't actually sleep during tests."""
    monkeypatch.setattr(auth.login.retry, "wait", lambda *a, **k: 0)


def _patch_garmin(monkeypatch, login_side_effects):
    """Patch ``auth.Garmin`` with a stub whose ``.login`` plays back side effects.

    Each call to the stub constructor returns a fresh client (mirrors login
    building a new client per attempt). Returns a counter dict for assertions.
    """
    counters = {"constructed": 0, "login_calls": 0}
    effects = iter(login_side_effects)

    class _StubClient:
        def __init__(self, **kwargs):
            counters["constructed"] += 1

        def login(self, tokenstore=None):
            counters["login_calls"] += 1
            outcome = next(effects)
            if isinstance(outcome, Exception):
                raise outcome
            return outcome

    monkeypatch.setattr(auth, "Garmin", _StubClient)
    return counters


@pytest.mark.unit
class TestLoginRetry:
    def test_succeeds_first_try(self, monkeypatch, settings):
        counters = _patch_garmin(monkeypatch, [None])
        auth.login(settings)
        assert counters["login_calls"] == 1
        assert counters["constructed"] == 1

    def test_retries_then_succeeds_on_transient_auth_error(self, monkeypatch, settings):
        # The real-world failure: JWT_WEB cookie not set after ticket consumption.
        counters = _patch_garmin(
            monkeypatch,
            [
                GarminConnectAuthenticationError(
                    "JWT_WEB cookie not set after ticket consumption"
                ),
                None,
            ],
        )
        auth.login(settings)
        assert counters["login_calls"] == 2
        # A fresh client is built per attempt.
        assert counters["constructed"] == 2

    def test_retries_on_connection_error(self, monkeypatch, settings):
        counters = _patch_garmin(
            monkeypatch,
            [GarminConnectConnectionError("connection reset"), None],
        )
        auth.login(settings)
        assert counters["login_calls"] == 2

    def test_gives_up_after_max_attempts_and_reraises(self, monkeypatch, settings):
        counters = _patch_garmin(
            monkeypatch,
            [GarminConnectAuthenticationError("still flaky")] * auth._LOGIN_MAX_ATTEMPTS,
        )
        with pytest.raises(GarminConnectAuthenticationError):
            auth.login(settings)
        assert counters["login_calls"] == auth._LOGIN_MAX_ATTEMPTS

    def test_does_not_retry_unexpected_error(self, monkeypatch, settings):
        counters = _patch_garmin(monkeypatch, [ValueError("not a garmin error")])
        with pytest.raises(ValueError):
            auth.login(settings)
        assert counters["login_calls"] == 1  # no retry on non-retryable exception
