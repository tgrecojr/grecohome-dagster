"""Tests that the Lingo code location's Definitions resolve cleanly."""

import pytest
from grecohome_lingo.config import settings
from grecohome_lingo.dagster.definitions import defs
from grecohome_lingo.dagster.sensors import lingo_drive_sensor


@pytest.mark.unit
class TestDefinitions:
    def test_single_asset(self):
        keys = {str(k) for k in defs.resolve_asset_graph().get_all_asset_keys()}
        assert keys == {"AssetKey(['lingo_bronze_glucose'])"}

    def test_job_resolves(self):
        assert defs.resolve_job_def("lingo_capture_job") is not None

    def test_sensor_config(self):
        assert lingo_drive_sensor.name == "lingo_drive_sensor"
        assert (
            lingo_drive_sensor.minimum_interval_seconds
            == settings.gdrive_poll_interval_minutes * 60
        )
