"""Tests for the bronze capture module (grecohome_core.bronze.capture)."""

import hashlib
import json
import os
import re

import pytest

from grecohome_core.bronze import capture_bronze
from grecohome_core.bronze.capture import _ext_for, _utc_now_ms

# {source}/{collection}/dt=YYYY-MM-DD/{collection}_{unix_ms}_{short_id}.{ext}
NAME_RE = re.compile(r"^whoop/recovery/dt=\d{4}-\d{2}-\d{2}/recovery_\d+_[0-9a-f]{6}\.json$")

REQUIRED_SIDECAR_KEYS = {
    "source",
    "collection",
    "fetched_at",
    "fetched_at_unix_ms",
    "request_url",
    "request_params",
    "http_status",
    "content_type",
    "byte_size",
    "sha256",
    "stored_encoding",
    "schema_version",
}


def _payloads(root: str) -> list[str]:
    return [
        os.path.join(d, n)
        for d, _s, names in os.walk(root)
        for n in names
        if n.endswith(".json") and not n.endswith(".meta.json")
    ]


@pytest.mark.unit
class TestBronzeHelpers:
    def test_ext_for_json_identity(self):
        assert _ext_for("application/json", "identity") == "json"
        assert _ext_for("application/json; charset=utf-8", "identity") == "json"

    def test_ext_for_native_gzip(self):
        assert _ext_for("text/csv", "gzip") == "csv.gz"

    def test_ext_for_unknown_falls_back_to_bin(self):
        assert _ext_for("application/octet-stream", "identity") == "bin"
        assert _ext_for(None, "identity") == "bin"

    def test_utc_now_ms_is_millis(self):
        assert _utc_now_ms() > 1_577_836_800_000  # well past 2020


@pytest.mark.unit
class TestCaptureBronze:
    def test_writes_payload_byte_for_byte_with_sidecar(self, tmp_path):
        root = str(tmp_path / "bronze")
        raw = b'{"records": [{"id": 1}], "next_token": null}'
        meta = {
            "request_url": "https://api.prod.whoop.com/developer/v2/recovery?limit=25",
            "request_params": {"limit": "25"},
            "http_status": 200,
            "content_type": "application/json",
            "stored_encoding": "identity",
            "processor_version": "1.0.0",
        }

        path = capture_bronze("whoop", "recovery", raw, meta, bronze_root=root)

        assert path is not None
        with open(path, "rb") as fh:
            assert fh.read() == raw

        rel = os.path.relpath(path, root).replace(os.sep, "/")
        assert NAME_RE.match(rel), rel

        sidecar = json.load(open(re.sub(r"\.json$", ".meta.json", path)))
        assert REQUIRED_SIDECAR_KEYS.issubset(sidecar.keys())
        assert sidecar["source"] == "whoop"
        assert sidecar["collection"] == "recovery"
        assert sidecar["http_status"] == 200
        assert sidecar["schema_version"] == "v1"
        assert sidecar["byte_size"] == len(raw)
        assert sidecar["sha256"] == hashlib.sha256(raw).hexdigest()
        assert sidecar["fetched_at"].endswith("Z")

    def test_sidecar_contains_no_secrets(self, tmp_path):
        root = str(tmp_path / "bronze")
        meta = {
            "request_url": "https://api.prod.whoop.com/developer/v2/recovery?limit=25",
            "request_params": {"limit": "25"},
            "http_status": 200,
            "content_type": "application/json",
        }
        path = capture_bronze("whoop", "recovery", b"{}", meta, bronze_root=root)
        text = open(re.sub(r"\.json$", ".meta.json", path)).read().lower()
        for needle in ("authorization", "bearer", "access_token", "secret"):
            assert needle not in text

    def test_capture_failure_is_non_fatal(self, tmp_path):
        # A non-bytes payload violates the invariant but must be swallowed.
        result = capture_bronze("whoop", "recovery", "not-bytes", {}, bronze_root=str(tmp_path))
        assert result is None

    def test_distinct_payloads_both_written(self, tmp_path):
        root = str(tmp_path / "bronze")
        meta = {"content_type": "application/json"}
        p1 = capture_bronze("whoop", "recovery", b'{"a":1}', meta, bronze_root=root)
        p2 = capture_bronze("whoop", "recovery", b'{"a":2}', meta, bronze_root=root)
        assert p1 and p2 and p1 != p2
        assert len(_payloads(root)) == 2

    def test_non_2xx_response_is_not_persisted(self, tmp_path):
        # An HTTP error body (e.g. a 401 auth envelope) must never land in bronze,
        # or it would poison a content/integrity check.
        root = str(tmp_path / "bronze")
        meta = {"http_status": 401, "content_type": "application/json"}
        result = capture_bronze(
            "whoop", "profile", b'"Authorization was not valid"', meta, bronze_root=root
        )
        assert result is None
        assert _payloads(root) == []

    def test_2xx_response_is_persisted(self, tmp_path):
        # The guard only rejects non-2xx; a normal 200 is written as usual.
        root = str(tmp_path / "bronze")
        meta = {"http_status": 200, "content_type": "application/json"}
        result = capture_bronze("whoop", "profile", b'{"user_id":1}', meta, bronze_root=root)
        assert result is not None
        assert len(_payloads(root)) == 1


@pytest.mark.unit
class TestContentHashDedup:
    def test_identical_payload_is_deduped(self, tmp_path):
        root = str(tmp_path / "bronze")
        raw = b'{"records": [{"id": 1}]}'
        meta = {"content_type": "application/json"}

        first = capture_bronze("whoop", "recovery", raw, meta, bronze_root=root)
        second = capture_bronze("whoop", "recovery", raw, meta, bronze_root=root)

        assert first is not None
        assert second is None  # identical payload skipped
        assert len(_payloads(root)) == 1

    def test_changed_payload_after_dedup_is_written(self, tmp_path):
        root = str(tmp_path / "bronze")
        meta = {"content_type": "application/json"}
        capture_bronze("whoop", "recovery", b'{"v":1}', meta, bronze_root=root)
        capture_bronze("whoop", "recovery", b'{"v":1}', meta, bronze_root=root)  # deduped
        third = capture_bronze("whoop", "recovery", b'{"v":2}', meta, bronze_root=root)
        assert third is not None
        assert len(_payloads(root)) == 2

    def test_dedup_is_per_partition_date(self, tmp_path):
        root = str(tmp_path / "bronze")
        raw = b'{"same": true}'
        meta = {"content_type": "application/json"}
        a = capture_bronze("whoop", "recovery", raw, meta, bronze_root=root, dt="2025-01-01")
        b = capture_bronze("whoop", "recovery", raw, meta, bronze_root=root, dt="2025-01-02")
        # Same payload, different partition dt -> both written (separate folders).
        assert a is not None and b is not None
        assert "dt=2025-01-01" in a
        assert "dt=2025-01-02" in b
        assert len(_payloads(root)) == 2

    def test_dt_override_routes_to_partition_folder(self, tmp_path):
        root = str(tmp_path / "bronze")
        path = capture_bronze(
            "whoop", "sleep", b"{}", {"content_type": "application/json"},
            bronze_root=root, dt="2024-12-31",
        )
        assert "/whoop/sleep/dt=2024-12-31/" in path.replace(os.sep, "/")
