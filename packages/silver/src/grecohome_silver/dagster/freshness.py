"""Table freshness: a silver table whose Parquet wasn't rewritten recently fails at ERROR.

A failed rebuild leaves the previous Parquet in place, where every structural check
still passes, and a failed run pages no one. Freshness makes a failed or never-enabled
rebuild loud. It measures the table's last write (file modification time), not the
newest data in it: a day with nothing new still rewrites the table.

At the daily 07:00 UTC checks run a table is ~1 h old normally, ~25 h after one missed
rebuild and ~49 h after two, so a 36 h limit pages on the second consecutive miss with
wide margin either side. See docs/intent/silver/silver-design.md (Asset checks).
"""

from __future__ import annotations

import os
import time
from collections.abc import Callable

from dagster import AssetCheckResult, AssetCheckSeverity, AssetsDefinition, asset_check

from grecohome_core.checks import alerting_check

MAX_AGE_HOURS = 36.0


def table_freshness_check(asset: AssetsDefinition, path_fn: Callable[[], str]):
    """An ERROR check ``<asset>_freshness`` on ``path_fn()``'s Parquet modification time.

    ``path_fn`` is called at check time (so a test or config change to the silver root
    applies). A missing table fails at ERROR, like every silver check.
    """
    name = f"{asset.key.to_user_string()}_freshness"

    @asset_check(asset=asset, name=name)
    @alerting_check(name=name, asset=asset.key)
    def _check() -> AssetCheckResult:
        path = path_fn()
        if not os.path.exists(path):
            return AssetCheckResult(
                passed=False,
                severity=AssetCheckSeverity.ERROR,
                metadata={"error": f"silver output missing: {path}"},
            )
        age_h = (time.time() - os.path.getmtime(path)) / 3600
        return AssetCheckResult(
            passed=age_h <= MAX_AGE_HOURS,
            severity=AssetCheckSeverity.ERROR,
            metadata={"age_hours": round(age_h, 2), "max_age_hours": MAX_AGE_HOURS},
        )

    return _check
