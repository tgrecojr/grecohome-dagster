"""Tests for the Lingo capture adapter."""

import json
import os
import re

import pytest
from grecohome_lingo.capture import capture_glucose

SAMPLE = b"2026-06-06T15:25-04:00,73\n2026-06-06T15:20-04:00,75\n"


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


@pytest.mark.unit
class TestCaptureGlucose:
    def test_writes_csv_with_provenance(self, tmp_path):
        root = str(tmp_path / "bronze")
        path = capture_glucose(
            SAMPLE, file_id="abc123", file_name="lingo.csv", folder_id="folder1",
            bronze_root=root, created_time="2026-06-06T00:00:00Z",
        )
        assert path is not None and path.endswith(".csv")
        assert "/lingo/glucose/dt=" in path.replace(os.sep, "/")
        with open(path, "rb") as fh:
            assert fh.read() == SAMPLE  # raw bytes, unmodified

        sidecar = json.load(open(re.sub(r"\.csv$", ".meta.json", path)))
        assert sidecar["source"] == "lingo"
        assert sidecar["collection"] == "glucose"
        assert sidecar["content_type"] == "text/csv"
        assert sidecar["processor"] == "grecohome-lingo"
        assert sidecar["request_params"]["file_id"] == "abc123"

    def test_no_secrets_in_sidecar(self, tmp_path):
        root = str(tmp_path / "bronze")
        path = capture_glucose(
            SAMPLE, file_id="x", file_name="g.csv", folder_id="f", bronze_root=root
        )
        text = open(re.sub(r"\.csv$", ".meta.json", path)).read().lower()
        for needle in ("token", "authorization", "secret", "credential", "bearer"):
            assert needle not in text

    def test_dedupe_skips_identical_reupload(self, tmp_path):
        root = str(tmp_path / "bronze")
        a = capture_glucose(SAMPLE, file_id="x", file_name="g.csv", folder_id="f", bronze_root=root)
        b = capture_glucose(
            SAMPLE, file_id="y", file_name="g2.csv", folder_id="f", bronze_root=root
        )
        assert a is not None
        assert b is None  # byte-identical content deduped
        assert len(_payloads(root)) == 1

    def test_changed_content_is_captured(self, tmp_path):
        root = str(tmp_path / "bronze")
        capture_glucose(SAMPLE, file_id="x", file_name="g.csv", folder_id="f", bronze_root=root)
        bigger = SAMPLE + b"2026-06-06T15:30-04:00,71\n"
        path = capture_glucose(
            bigger, file_id="y", file_name="g2.csv", folder_id="f", bronze_root=root
        )
        assert path is not None
        assert len(_payloads(root)) == 2
