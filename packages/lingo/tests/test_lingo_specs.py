"""Tests for the Lingo safeguards: failed writes, required settings, folder health."""

import os
import subprocess
import sys
from unittest.mock import MagicMock, patch

import pytest
from dagster import AssetCheckSeverity, DagsterInstance, materialize
from grecohome_lingo.dagster.assets import lingo_bronze_glucose


def _service() -> MagicMock:
    service = MagicMock()
    service.files.return_value.get.return_value.execute.return_value = {
        "id": "f1",
        "name": "lingo.csv",
        "createdTime": "2026-06-06T00:00:00Z",
        "modifiedTime": "2026-06-06T01:00:00Z",
    }
    return service


@pytest.mark.integration
class TestFailedWrite:
    # @spec LINGO-CAPTURE-004
    def test_a_bronze_write_that_returns_no_path_fails_the_run(self):
        inst = DagsterInstance.ephemeral()
        inst.add_dynamic_partitions("lingo_files", ["f1"])
        with (
            patch("grecohome_lingo.drive.get_drive_service", return_value=_service()),
            patch("grecohome_lingo.drive.download_file_bytes", return_value=b"x,1\n"),
            patch("grecohome_lingo.dagster.assets.capture_glucose", return_value=None),
        ):
            result = materialize(
                [lingo_bronze_glucose], partition_key="f1", instance=inst, raise_on_error=False
            )
        assert not result.success


    # @spec LINGO-CAPTURE-003
    def test_capture_records_its_metadata(self):
        inst = DagsterInstance.ephemeral()
        inst.add_dynamic_partitions("lingo_files", ["f1"])
        with (
            patch("grecohome_lingo.drive.get_drive_service", return_value=_service()),
            patch("grecohome_lingo.drive.download_file_bytes", return_value=b"x,1\n"),
        ):
            result = materialize([lingo_bronze_glucose], partition_key="f1", instance=inst)
        (mat,) = result.asset_materializations_for_node("lingo_bronze_glucose")
        md = {k: v.value for k, v in mat.metadata.items()}
        assert (md["file_id"], md["file_name"], md["bytes"]) == ("f1", "lingo.csv", 4)
        assert md["bronze_path"].endswith(".csv")


@pytest.mark.unit
class TestRequiredSettingsAtStartup:
    # @spec LINGO-AUTH-002
    def test_code_location_refuses_to_start_without_the_folder_id(self, tmp_path):
        env = {k: v for k, v in os.environ.items() if not k.startswith("GDRIVE_")}
        env["GDRIVE_SERVICE_ACCOUNT_PATH"] = "/tmp/sa.json"
        proc = subprocess.run(
            [sys.executable, "-c", "import grecohome_lingo.dagster.definitions"],
            env=env,
            capture_output=True,
            text=True,
            timeout=120,
            cwd=tmp_path,  # no local .env can supply the missing value
        )
        assert proc.returncode != 0
        assert "GDRIVE_FOLDER_ID" in proc.stderr


@pytest.mark.unit
class TestFolderHealth:
    def _run(self, **list_kwargs):
        from grecohome_lingo.dagster import checks

        with (
            patch("grecohome_lingo.drive.get_drive_service", return_value=MagicMock()),
            patch("grecohome_lingo.drive.list_csv_files", **list_kwargs),
            patch("grecohome_core.checks.alerting._logger.error") as alert,
        ):
            result = checks.lingo_drive_folder_health()
        return result, alert

    # @spec LINGO-CHECKS-002
    def test_passes_when_the_folder_lists_csv_files(self):
        result, alert = self._run(return_value=[{"id": "f1", "name": "a.csv"}])
        assert result.passed and result.severity == AssetCheckSeverity.ERROR
        assert not alert.called

    @pytest.mark.parametrize(
        "list_kwargs", [{"return_value": []}, {"side_effect": RuntimeError("boom")}]
    )
    def test_fails_and_alerts_when_empty_or_listing_errors(self, list_kwargs):
        result, alert = self._run(**list_kwargs)
        assert not result.passed and result.severity == AssetCheckSeverity.ERROR
        assert alert.call_args.args[0] == "asset_check_failed"

    # @spec LINGO-CHECKS-003
    def test_reports_no_credentials_or_error_text(self):
        result, _ = self._run(side_effect=RuntimeError("private_key=SECRETVALUE"))
        reported = " ".join(f"{k}={v.value}" for k, v in result.metadata.items())
        assert "SECRETVALUE" not in reported and "RuntimeError" in reported
        assert "/tmp/grecohome-test-sa.json" not in reported
