"""Tests for the alerting_check decorator (emits asset_check_failed on ERROR fails)."""

from unittest.mock import patch

import pytest
from dagster import AssetCheckResult, AssetCheckSeverity

from grecohome_core.checks.alerting import alerting_check

pytestmark = pytest.mark.unit


def _err(passed: bool) -> AssetCheckResult:
    return AssetCheckResult(passed=passed, severity=AssetCheckSeverity.ERROR)


def _warn(passed: bool) -> AssetCheckResult:
    return AssetCheckResult(passed=passed, severity=AssetCheckSeverity.WARN)


# @spec CORE-CHECKS-ALERT-001
def test_failing_error_check_logs_signal():
    @alerting_check(name="x_freshness", asset="AssetKey(['x'])")
    def chk() -> AssetCheckResult:
        return _err(False)

    with patch("grecohome_core.checks.alerting._logger.error") as err:
        result = chk()

    assert result.passed is False
    err.assert_called_once()
    assert err.call_args.args[0] == "asset_check_failed"
    assert err.call_args.kwargs["check"] == "x_freshness"
    assert err.call_args.kwargs["asset"] == "AssetKey(['x'])"


def test_passing_check_does_not_log():
    @alerting_check
    def chk() -> AssetCheckResult:
        return _err(True)

    with patch("grecohome_core.checks.alerting._logger.error") as err:
        chk()
    err.assert_not_called()


# @spec CORE-CHECKS-ALERT-001
def test_failing_warn_check_does_not_log():
    # WARN coverage/expectation failures stay UI-only — they must not page.
    @alerting_check
    def chk() -> AssetCheckResult:
        return _warn(False)

    with patch("grecohome_core.checks.alerting._logger.error") as err:
        chk()
    err.assert_not_called()


def test_bare_form_uses_function_name():
    @alerting_check
    def my_named_check() -> AssetCheckResult:
        return _err(False)

    with patch("grecohome_core.checks.alerting._logger.error") as err:
        my_named_check()
    assert err.call_args.kwargs["check"] == "my_named_check"


# @spec CORE-CHECKS-ALERT-002
def test_logging_failure_never_breaks_the_check():
    @alerting_check
    def chk() -> AssetCheckResult:
        return _err(False)

    with patch("grecohome_core.checks.alerting._logger.error", side_effect=RuntimeError("boom")):
        result = chk()  # must not raise
    assert result.passed is False


# @spec CORE-CHECKS-ALERT-004
def test_raising_check_logs_signal_and_reraises():
    # A check that crashes (a corrupt file it reads, say) must page, not just fail a step.
    @alerting_check(name="x_coverage", asset="AssetKey(['x'])")
    def chk() -> AssetCheckResult:
        raise RuntimeError("corrupt input")

    with patch("grecohome_core.checks.alerting._logger.error") as err:
        with pytest.raises(RuntimeError, match="corrupt input"):
            chk()

    err.assert_called_once()
    assert err.call_args.args[0] == "asset_check_failed"
    assert err.call_args.kwargs["check"] == "x_coverage"
    assert err.call_args.kwargs["asset"] == "AssetKey(['x'])"


# @spec CORE-CHECKS-ALERT-004
def test_raising_check_reraises_even_if_logging_fails():
    @alerting_check
    def chk() -> AssetCheckResult:
        raise ValueError("boom")

    with patch("grecohome_core.checks.alerting._logger.error", side_effect=OSError("down")):
        with pytest.raises(ValueError, match="boom"):
            chk()
