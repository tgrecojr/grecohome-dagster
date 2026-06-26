"""Structured logging configuration using structlog.

Source-agnostic: :func:`configure_logging` takes the level/environment explicitly
(a subject passes its own settings) rather than importing a global ``settings``,
so this module has no dependency on any particular subject's config.
"""

import logging
import sys
from typing import Any

import structlog
from pythonjsonlogger.json import JsonFormatter


def configure_logging(log_level: str = "INFO", environment: str = "development") -> None:
    """Configure structured logging.

    Args:
        log_level: Standard logging level name (e.g. ``"INFO"``, ``"DEBUG"``).
        environment: When ``"production"`` logs render as JSON; otherwise a
            colorized console renderer is used for local readability.
    """
    level = getattr(logging, log_level.upper(), logging.INFO)

    json_formatter = JsonFormatter(
        fmt="%(asctime)s %(name)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
    )

    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(json_formatter)
    root_logger.addHandler(console_handler)

    # Never render frame-local variables in tracebacks. Secrets (e.g. the Whoop
    # OAuth access/refresh tokens) live in locals on the refresh path, and the
    # dev rich-traceback formatter defaults to dumping them verbatim into logs.
    # Forcing show_locals=False keeps any stray exc_info=True from leaking them.
    renderer = (
        structlog.processors.JSONRenderer()
        if environment == "production"
        else structlog.dev.ConsoleRenderer(
            colors=True,
            exception_formatter=structlog.dev.RichTracebackFormatter(show_locals=False),
        )
    )

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.StackInfoRenderer(),
            structlog.dev.set_exc_info,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            renderer,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(level),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )

    # Reduce noise from third-party HTTP libraries.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)


def get_logger(name: str) -> Any:
    """Return a structured logger bound to ``name`` (typically ``__name__``)."""
    return structlog.get_logger(name)
