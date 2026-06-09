"""Tests for SoilSettings."""

import pytest
from grecohome_soil.config import SoilSettings


@pytest.mark.unit
class TestSoilSettings:
    def test_inherits_bronze_root(self):
        # BRONZE_ROOT comes from the test env (root pyproject pytest-env).
        assert SoilSettings().bronze_root

    def test_defaults(self, monkeypatch):
        for var in (
            "USCRN_STATION",
            "USCRN_BASE_URL",
            "USCRN_LOOKBACK_DAYS",
            "USCRN_START_DATE",
        ):
            monkeypatch.delenv(var, raising=False)
        s = SoilSettings()
        assert s.uscrn_station == "PA_Avondale_2_N"
        assert s.uscrn_base_url.endswith("/hourly02")
        assert s.uscrn_lookback_days == 2
        assert s.uscrn_start_date == "2010-01-01"

    def test_reads_env(self, monkeypatch):
        monkeypatch.setenv("USCRN_STATION", "CA_Bodega_6_WSW")
        monkeypatch.setenv("USCRN_LOOKBACK_DAYS", "5")
        s = SoilSettings()
        assert s.uscrn_station == "CA_Bodega_6_WSW"
        assert s.uscrn_lookback_days == 5
