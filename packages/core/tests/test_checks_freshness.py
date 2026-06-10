"""Tests for the fetch-freshness asset check."""

import pytest
from dagster import AssetCheckKey, AssetCheckSeverity, AssetKey
from freezegun import freeze_time

from grecohome_core.checks import build_fetch_freshness_check
from grecohome_core.checks.config import CollectionCheckConfig

KEY = AssetKey("whoop_bronze_sleep")


def _cfg(**kw) -> CollectionCheckConfig:
    base = dict(
        source="whoop", collection="sleep", asset_key=KEY,
        cadence_hours=26.0, grace_hours=6.0,
    )
    base.update(kw)
    return CollectionCheckConfig(**base)


@pytest.mark.unit
class TestFreshness:
    def test_fresh_passes(self, capture, bronze_root):
        capture("whoop", "sleep", {"records": [{"a": 1}]}, dt="2024-12-01",
                fetched="2024-12-01T10:00:00")
        check = build_fetch_freshness_check(_cfg(), bronze_root)
        with freeze_time("2024-12-01T15:00:00"):  # 5h later, well within 32h
            res = check()
        assert res.passed
        assert res.severity == AssetCheckSeverity.ERROR

    def test_stale_fails_error(self, capture, bronze_root):
        capture("whoop", "sleep", {"records": [{"a": 1}]}, dt="2024-12-01",
                fetched="2024-12-01T10:00:00")
        check = build_fetch_freshness_check(_cfg(), bronze_root)
        with freeze_time("2024-12-03T10:00:00"):  # 48h later, beyond 32h
            res = check()
        assert not res.passed
        assert res.severity == AssetCheckSeverity.ERROR

    def test_no_captures_fails(self, bronze_root):
        check = build_fetch_freshness_check(_cfg(), bronze_root)
        res = check()
        assert not res.passed

    def test_no_captures_expected_empty_passes(self, bronze_root):
        check = build_fetch_freshness_check(_cfg(expected_empty=True), bronze_root)
        res = check()
        assert res.passed  # legitimately-empty collections write nothing

    def test_check_key_attaches_to_asset(self, bronze_root):
        check = build_fetch_freshness_check(_cfg(), bronze_root)
        assert AssetCheckKey(asset_key=KEY, name="whoop_sleep_freshness") in check.check_keys
