"""Tests for the derived-layer table freshness check builder."""

from __future__ import annotations

import os
import time
from unittest.mock import patch

import pytest
from dagster import AssetCheckSeverity, AssetKey, asset

from grecohome_core.checks.table_freshness import MAX_AGE_HOURS, table_freshness_check

pytestmark = pytest.mark.unit


@asset(name="layer_probe")
def _probe() -> None:
    """A stand-in asset for building a freshness check."""


def _check(path: str):
    return table_freshness_check(_probe, lambda: path)


# @spec CORE-CHECKS-TABLE-001
def test_freshness_name_age_and_limit(tmp_path) -> None:
    path = str(tmp_path / "t.parquet")
    open(path, "w").close()
    check = _check(path)
    assert {s.name for s in check.check_specs} == {"layer_probe_freshness"}
    assert {s.asset_key for s in check.check_specs} == {AssetKey("layer_probe")}
    for age_h, passed in ((0, True), (MAX_AGE_HOURS, True), (MAX_AGE_HOURS + 0.5, False)):
        old = time.time() - age_h * 3600 + (1 if age_h == MAX_AGE_HOURS else 0)
        os.utime(path, (old, old))
        r = check()
        assert r.passed is passed and r.severity == AssetCheckSeverity.ERROR
        assert r.metadata["max_age_hours"].value == 36.0 and "age_hours" in r.metadata


# @spec CORE-CHECKS-TABLE-001
def test_path_is_read_when_the_check_runs(tmp_path) -> None:
    where = {"path": str(tmp_path / "absent.parquet")}
    check = table_freshness_check(_probe, lambda: where["path"])
    assert not check().passed
    where["path"] = str(tmp_path / "t.parquet")
    open(where["path"], "w").close()
    assert check().passed


# @spec CORE-CHECKS-TABLE-002, CORE-CHECKS-TABLE-003
def test_missing_table_fails_at_error_and_alerts(tmp_path) -> None:
    check = _check(str(tmp_path / "absent.parquet"))
    with patch("grecohome_core.checks.alerting._logger.error") as err:
        r = check()
    assert not r.passed and r.severity == AssetCheckSeverity.ERROR
    err.assert_called_once()
    assert err.call_args.kwargs["check"] == "layer_probe_freshness"
