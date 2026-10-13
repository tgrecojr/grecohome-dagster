"""Make a failing asset check emit a distinct, alertable log line.

Dagster OSS surfaces a failed ``@asset_check`` only in the UI (the result is a
framework event in Postgres, not a log line), so there's nothing for an external
alerting stack to watch. This decorator closes that gap: when a wrapped check returns
a **failing, ERROR-severity** result it logs one structured ``asset_check_failed``
line via the app's structlog (which already ships to Loki). A single Grafana Loki →
Slack rule on that token then covers every layer's page-worthy checks — bronze
capture freshness/schema drift, silver/gold uniqueness/range — with near-zero false
positives, because the signal only fires on a real failed check (not framework noise).

WARN-severity failures (coverage/expectation drift) are intentionally *not* emitted —
they stay UI-only and don't page. A check that *raises* emits the same line
before the exception propagates, so a crash pages like a clean failure. Wrap the
inner function **below** ``@asset_check``::

    @asset_check(asset=..., name="my_check")
    @alerting_check                      # bare: name taken from the function
    def my_check() -> AssetCheckResult: ...

    @asset_check(...)
    @alerting_check(name="x_freshness", asset=cfg.asset_key)   # parametrized
    def _check() -> AssetCheckResult: ...
"""

from __future__ import annotations

import functools
from collections.abc import Callable

from dagster import AssetCheckResult, AssetCheckSeverity

from grecohome_core.logging_config import get_logger

_logger = get_logger("grecohome_core.checks.alerting")


# @spec CORE-CHECKS-ALERT-001, CORE-CHECKS-ALERT-002, CORE-CHECKS-ALERT-004
def alerting_check(_fn: Callable | None = None, *, name: str | None = None, asset: object = None):
    """Decorate an asset-check function so a failing ERROR result logs ``asset_check_failed``.

    Usable bare (``@alerting_check``; the check name is the function name) or
    parametrized (``@alerting_check(name=..., asset=...)`` — used by the generic
    bronze builders, which name checks per collection). A check that raises logs the
    same line and the exception is re-raised. Never lets logging break a check: any
    error while emitting is swallowed.
    """

    def deco(fn: Callable) -> Callable:
        check_name = name or getattr(fn, "__name__", "asset_check")

        def _signal() -> None:
            fields = {"check": check_name}
            if asset is not None:
                fields["asset"] = str(asset)
            _logger.error("asset_check_failed", **fields)

        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            try:
                result = fn(*args, **kwargs)
            except Exception:
                # A check that crashes pages like one that fails; the run still fails.
                try:
                    _signal()
                except Exception:  # noqa: BLE001 - alerting must never mask the error
                    pass
                raise
            try:
                if (
                    isinstance(result, AssetCheckResult)
                    and not result.passed
                    and result.severity == AssetCheckSeverity.ERROR
                ):
                    _signal()
            except Exception:  # noqa: BLE001 - alerting must never break a check
                pass
            return result

        return wrapper

    return deco(_fn) if _fn is not None else deco
