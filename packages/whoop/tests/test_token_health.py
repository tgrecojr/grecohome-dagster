"""Tests for the Whoop token-health asset check."""

from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import pytest
from dagster import AssetCheckSeverity

from grecohome_whoop.dagster.checks import evaluate_token_health, whoop_token_health

pytestmark = pytest.mark.unit

NOW = datetime(2026, 6, 13, 12, 0, 0, tzinfo=UTC)


def _tok(expires_at: str) -> dict:
    return {
        "access_token": "a",
        "refresh_token": "r",
        "token_type": "Bearer",
        "expires_at": expires_at,
        "scopes": [],
    }


class TestEvaluateTokenHealth:
    def test_fresh_token_passes(self):
        passed, meta = evaluate_token_health(_tok((NOW + timedelta(minutes=55)).isoformat()), NOW)
        assert passed is True
        assert meta["minutes_past_expiry"] < 0

    def test_expired_within_grace_passes(self):
        # Expired 30 min ago — normal between hourly runs, not an alert.
        passed, _ = evaluate_token_health(_tok((NOW - timedelta(minutes=30)).isoformat()), NOW)
        assert passed is True

    def test_expired_beyond_grace_fails(self):
        passed, meta = evaluate_token_health(_tok((NOW - timedelta(hours=2)).isoformat()), NOW)
        assert passed is False
        assert meta["minutes_past_expiry"] == pytest.approx(120.0)

    @pytest.mark.parametrize("data", [None, {}, {"access_token": "a"}])
    def test_missing_or_no_expiry_fails(self, data):
        passed, meta = evaluate_token_health(data, NOW)
        assert passed is False and "error" in meta

    def test_unparseable_expiry_fails(self):
        passed, meta = evaluate_token_health(_tok("not-a-date"), NOW)
        assert passed is False and "error" in meta

    def test_naive_expiry_assumed_utc(self):
        # Naive timestamp far in the past -> treated as UTC -> fails.
        passed, _ = evaluate_token_health(_tok("2020-01-01T00:00:00"), NOW)
        assert passed is False


class TestWhoopTokenHealthCheck:
    def test_check_passes_on_fresh_token(self):
        fresh = _tok((datetime.now(UTC) + timedelta(minutes=55)).isoformat())
        with patch("grecohome_whoop.dagster.checks.TokenFileStore.read", return_value=fresh):
            result = whoop_token_health()
        assert result.passed is True
        assert result.severity == AssetCheckSeverity.ERROR

    def test_check_fails_and_logs_on_stale_token(self):
        stale = _tok((datetime.now(UTC) - timedelta(hours=3)).isoformat())
        with patch("grecohome_whoop.dagster.checks.TokenFileStore.read", return_value=stale), patch(
            "grecohome_whoop.dagster.checks.logger.warning"
        ) as warn:
            result = whoop_token_health()
        assert result.passed is False
        warn.assert_called_once()
        assert warn.call_args.args[0] == "whoop_token_unhealthy"

    def test_check_fails_when_token_file_missing(self):
        with patch("grecohome_whoop.dagster.checks.TokenFileStore.read", return_value=None):
            result = whoop_token_health()
        assert result.passed is False
