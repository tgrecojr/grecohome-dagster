"""Structured logging configuration using structlog.

Source-agnostic: :func:`configure_logging` takes the level/environment explicitly
(a subject passes its own settings) rather than importing a global ``settings``,
so this module has no dependency on any particular subject's config.

Tracebacks never render frame-local variables, in any process. Secrets (OAuth
tokens, client secrets) live in locals on credential paths, and Rich's traceback
formatter -- structlog's default -- prints locals verbatim into the logs that ship
to Loki. :func:`get_logger` therefore applies safe defaults on first use when
nothing has configured structlog, so the guarantee doesn't depend on every code
location remembering to call :func:`configure_logging`.
"""

import logging
import os
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

    renderer = (
        structlog.processors.JSONRenderer()
        if environment == "production"
        else _console_renderer()
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


def _use_colors() -> bool:
    """structlog's own default color decision: ``NO_COLOR``, ``FORCE_COLOR``, else a TTY."""
    if os.environ.get("NO_COLOR"):
        return False
    if os.environ.get("FORCE_COLOR"):
        return True
    return sys.stdout is not None and hasattr(sys.stdout, "isatty") and sys.stdout.isatty()


def _console_renderer() -> structlog.dev.ConsoleRenderer:
    """A console renderer whose tracebacks never show frame-local variables.

    Colors follow structlog's default decision, so a container (no TTY) gets plain
    text, exactly like structlog's own default renderer -- only the locals go.
    """
    colors = _use_colors()
    return structlog.dev.ConsoleRenderer(
        colors=colors,
        force_colors=bool(os.environ.get("FORCE_COLOR")),
        exception_formatter=structlog.dev.RichTracebackFormatter(
            show_locals=False, color_system="truecolor" if colors else None
        ),
    )


def _ensure_safe_defaults() -> None:
    """Apply structlog's defaults minus traceback locals, unless already configured.

    Only the console renderer is swapped; every other default (processors, logger
    factory, ``cache_logger_on_first_use=False``) is kept, so the log format is
    unchanged and a later :func:`configure_logging` (or Dagster's own setup) still
    takes effect.
    """
    if structlog.is_configured():
        return
    processors = [
        _console_renderer() if isinstance(p, structlog.dev.ConsoleRenderer) else p
        for p in structlog.get_config()["processors"]
    ]
    structlog.configure(processors=processors)


# @spec WHOOP-LOG-005
def get_logger(name: str) -> Any:
    """Return a structured logger bound to ``name`` (typically ``__name__``).

    Applies the safe (no traceback locals) defaults first if nothing has
    configured structlog in this process.
    """
    _ensure_safe_defaults()
    return structlog.get_logger(name)
