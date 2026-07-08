"""Tests for GeocodeSettings."""

import pytest

from grecohome_geocode.config import GeocodeSettings

pytestmark = pytest.mark.unit


class TestGeocodeSettings:
    def test_inherits_bronze_root(self):
        assert GeocodeSettings().bronze_root

    def test_requires_photon_base_url(self):
        # PHOTON_BASE_URL comes from the root pytest-env; it must be present.
        assert GeocodeSettings().photon_base_url

    def test_defaults(self, monkeypatch):
        for var in (
            "PHOTON_TIMEOUT",
            "PHOTON_LANGUAGE",
            "PHOTON_RADIUS_KM",
            "GEOCODE_SCAN_DAYS",
            "GEOCODE_MAX_LOOKUPS_PER_RUN",
            "GEOCODE_RECENT_PARTITIONS",
        ):
            monkeypatch.delenv(var, raising=False)
        s = GeocodeSettings()
        assert s.photon_timeout == 30.0
        assert s.photon_language == "en"
        assert s.photon_radius_km == 0.05
        assert s.geocode_scan_days == 7
        assert s.geocode_max_lookups_per_run == 2000
        assert s.geocode_recent_partitions == 14

    def test_reads_env(self, monkeypatch):
        monkeypatch.setenv("PHOTON_BASE_URL", "http://elsewhere:9999")
        monkeypatch.setenv("GEOCODE_SCAN_DAYS", "30")
        s = GeocodeSettings()
        assert s.photon_base_url == "http://elsewhere:9999"
        assert s.geocode_scan_days == 30
