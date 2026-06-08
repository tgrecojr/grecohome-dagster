"""Tests for logging configuration."""

import logging

import pytest

from grecohome_core.logging_config import configure_logging, get_logger


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
