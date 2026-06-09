"""Test materializing the lingo asset writes bronze (mocked Drive)."""

import os
from unittest.mock import MagicMock, patch

import pytest
from dagster import DagsterInstance, materialize
from grecohome_lingo.dagster.assets import lingo_bronze_glucose


def _payloads(root: str) -> list[str]:
    base = os.path.join(root, "lingo", "glucose")
    if not os.path.isdir(base):
        return []
    return [
        os.path.join(d, n)
        for d, _s, names in os.walk(base)
        for n in names
        if not n.endswith(".meta.json")
    ]


@pytest.mark.integration
class TestMaterialize:
    def test_downloads_and_captures_partition(self, isolate_lingo_bronze):
        inst = DagsterInstance.ephemeral()
        inst.add_dynamic_partitions("lingo_files", ["f1"])

        service = MagicMock()
        service.files.return_value.get.return_value.execute.return_value = {
            "id": "f1",
            "name": "lingo.csv",
            "createdTime": "2026-06-06T00:00:00Z",
            "modifiedTime": "2026-06-06T01:00:00Z",
        }
        with (
            patch("grecohome_lingo.drive.get_drive_service", return_value=service),
            patch(
                "grecohome_lingo.drive.download_file_bytes",
                return_value=b"2026-06-06T15:25-04:00,73\n",
            ),
        ):
            result = materialize([lingo_bronze_glucose], partition_key="f1", instance=inst)

        assert result.success
        files = _payloads(isolate_lingo_bronze)
        assert len(files) == 1
        assert files[0].endswith(".csv")
        assert "/lingo/glucose/dt=" in files[0].replace(os.sep, "/")
