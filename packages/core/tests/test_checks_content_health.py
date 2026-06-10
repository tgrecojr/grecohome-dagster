"""Tests for the content-health asset check (classification + folded integrity)."""

import pytest
from dagster import AssetCheckSeverity, AssetKey

from grecohome_core.checks import build_content_health_check
from grecohome_core.checks.bronze_reads import read_sidecar, sidecar_for
from grecohome_core.checks.config import CollectionCheckConfig

KEY = AssetKey("garmin_bronze_hrv")


def _cfg(**kw) -> CollectionCheckConfig:
    base = dict(source="garmin", collection="hrv", asset_key=KEY,
                reader="json", unnest_records=False)
    base.update(kw)
    return CollectionCheckConfig(**base)


@pytest.mark.unit
class TestContentHealth:
    def test_data_passes(self, capture, bronze_root):
        capture("garmin", "hrv", {"hrvSummary": {"v": 1}}, dt="2024-12-01")
        res = build_content_health_check(_cfg(), bronze_root)()
        assert res.passed
        assert res.severity == AssetCheckSeverity.WARN

    def test_unexpected_empty_warns(self, capture, bronze_root):
        capture("garmin", "hrv", [], dt="2024-12-01")  # EMPTY_LIST
        res = build_content_health_check(_cfg(expected_empty=False), bronze_root)()
        assert not res.passed
        assert res.severity == AssetCheckSeverity.WARN

    def test_expected_empty_passes_on_empty(self, capture, bronze_root):
        capture("garmin", "hrv", [], dt="2024-12-01")  # EMPTY_LIST
        res = build_content_health_check(_cfg(expected_empty=True), bronze_root)()
        assert res.passed  # legitimately-empty hardware-unsupported collection

    def test_error_envelope_warns_even_if_expected_empty(self, capture, bronze_root):
        capture("garmin", "hrv", {"error": "nope"}, dt="2024-12-01")  # ERROR_LIKE
        res = build_content_health_check(_cfg(expected_empty=True), bronze_root)()
        assert not res.passed  # an error is not the same as "empty"

    def test_corruption_is_error(self, capture, bronze_root):
        path = capture("garmin", "hrv", {"hrvSummary": {"v": 1}}, dt="2024-12-01")
        with open(path, "ab") as fh:  # tamper -> sha/byte_size mismatch
            fh.write(b"junk")
        res = build_content_health_check(_cfg(), bronze_root)()
        assert not res.passed
        assert res.severity == AssetCheckSeverity.ERROR
        assert res.metadata["integrity_issues"].value >= 1

    def test_no_payloads_passes(self, bronze_root):
        # Absence is freshness's concern, not content's.
        res = build_content_health_check(_cfg(), bronze_root)()
        assert res.passed

    def test_http_error_warns(self, capture, bronze_root):
        # Force a non-2xx sidecar by editing it after capture.
        import json
        path = capture("garmin", "hrv", {"hrvSummary": {}}, dt="2024-12-01")
        meta_path = sidecar_for(path)
        sidecar = read_sidecar(path)
        sidecar["http_status"] = 503
        with open(meta_path, "w") as fh:
            json.dump(sidecar, fh)
        res = build_content_health_check(_cfg(expected_empty=True), bronze_root)()
        assert not res.passed
