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


@pytest.mark.unit
class TestAssetChecks:
    def _checks(self):
        ag = defs.resolve_asset_graph()
        return {(str(k.asset_key), k.name) for k in ag.asset_check_keys}

    def test_glucose_checks_no_freshness(self):
        # Intermittent wear -> freshness is intentionally disabled.
        keys = self._checks()
        akey = "AssetKey(['lingo_bronze_glucose'])"
        assert (akey, "lingo_glucose_completeness") in keys
        assert (akey, "lingo_glucose_schema_drift") in keys
        assert (akey, "lingo_glucose_content_health") in keys
        assert (akey, "lingo_glucose_freshness") not in keys

    def test_event_date_uses_csv_timestamp_column(self):
        from grecohome_lingo.dagster.checks import GLUCOSE_TS_COLUMN, LINGO_CHECK_CONFIGS

        cfg = LINGO_CHECK_CONFIGS[0]
        assert cfg.event_date_source == "payload"
        assert cfg.event_date_field == GLUCOSE_TS_COLUMN
        assert "Time of Glucose Reading" in GLUCOSE_TS_COLUMN

    def test_validation_job_registered(self):
        assert defs.resolve_job_def("lingo_bronze_checks_job") is not None
