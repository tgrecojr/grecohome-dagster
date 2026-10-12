"""Tests for logging configuration."""

import logging

import pytest
import structlog

from grecohome_core.logging_config import configure_logging, get_logger


@pytest.fixture(autouse=True)
def _restore_structlog_config():
    """Tests here reset or configure structlog globally; put it back afterwards."""
    was_configured = structlog.is_configured()
    saved = structlog.get_config()
    yield
    structlog.reset_defaults()
    if was_configured:
        structlog.configure(**saved)


# Built at runtime so it never appears in the traceback's source-code context --
# only a rendered frame-local variable could reveal it.
SECRET = "-".join(["s3cr3t", "local", "value"])


def _raise_with_local_value(local_value):  # noqa: ARG001 - must never appear in output
    raise RuntimeError("boom")


@pytest.mark.unit
class TestLogging:
    def test_configure_logging_sets_level(self):
        configure_logging(log_level="DEBUG", environment="development")
        assert logging.getLogger().level == logging.DEBUG

    def test_configure_logging_production_is_json(self):
        # Just exercise the production (JSON renderer) path without error.
        configure_logging(log_level="INFO", environment="production")
        assert logging.getLogger().level == logging.INFO

    def test_get_logger_returns_bound_logger(self):
        log = get_logger(__name__)
        # structlog loggers expose the standard level methods.
        assert hasattr(log, "info") and hasattr(log, "warning")


@pytest.mark.unit
class TestSafeDefaults:
    # @spec WHOOP-LOG-005
    def test_unconfigured_logger_renders_tracebacks_without_locals(self, capsys):
        structlog.reset_defaults()
        log = get_logger("safe-defaults-test")
        try:
            _raise_with_local_value(SECRET)
        except RuntimeError:
            log.error("failed", exc_info=True)
        out = capsys.readouterr().out
        assert "RuntimeError" in out and "boom" in out  # the traceback is still rendered
        assert SECRET not in out
        assert "\x1b[" not in out  # no color codes when output isn't a terminal

    def test_safe_defaults_leave_logger_caching_off(self):
        # A later configure_logging() must still be able to take effect.
        structlog.reset_defaults()
        get_logger("safe-defaults-test")
        assert structlog.is_configured()
        assert structlog.get_config()["cache_logger_on_first_use"] is False

    def test_existing_configuration_is_left_alone(self):
        structlog.reset_defaults()
        structlog.configure(cache_logger_on_first_use=True)
        get_logger("safe-defaults-test")
        assert structlog.get_config()["cache_logger_on_first_use"] is True
