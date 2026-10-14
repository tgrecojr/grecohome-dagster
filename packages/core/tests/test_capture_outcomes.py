"""capture_bronze outcomes, per-request skip-identical, sidecar rollback and collision safety."""

from __future__ import annotations

import glob
import os

import pytest

from grecohome_core.bronze import capture as capture_mod
from grecohome_core.bronze import capture_bronze
from grecohome_core.bronze.capture import CaptureOutcome

pytestmark = pytest.mark.unit


def _files(root) -> list[str]:
    return sorted(glob.glob(os.path.join(str(root), "**", "*"), recursive=True))


def _payloads(root) -> list[str]:
    return [f for f in _files(root) if os.path.isfile(f) and not f.endswith(".meta.json")]


def _cap(root, raw: bytes, params: dict | None = None, **kw):
    meta = {"content_type": "application/json"}
    if params is not None:
        meta["request_params"] = params
    return capture_bronze("garmin", "activity_details", raw, meta,
                          bronze_root=str(root), dt="2026-10-01", **kw)


# @spec CORE-BRONZE-012
def test_written_outcome_carries_the_path(tmp_path) -> None:
    r = _cap(tmp_path, b'{"a":1}')
    assert r.outcome == CaptureOutcome.WRITTEN and r.outcome == "written"
    assert r.path is not None and os.path.isfile(r.path)


# @spec CORE-BRONZE-009
def test_identical_capture_reports_skipped_identical(tmp_path) -> None:
    _cap(tmp_path, b'{"a":1}')
    r = _cap(tmp_path, b'{"a":1}')
    assert r.outcome == CaptureOutcome.SKIPPED_IDENTICAL and r.path is None
    assert len(_payloads(tmp_path)) == 1


# @spec CORE-BRONZE-008
def test_non_2xx_reports_skipped_non_2xx(tmp_path) -> None:
    r = capture_bronze("whoop", "profile", b"{}", {"http_status": 401},
                       bronze_root=str(tmp_path))
    assert r.outcome == CaptureOutcome.SKIPPED_NON_2XX and r.path is None


# @spec CORE-BRONZE-006
def test_failure_reports_failed(tmp_path) -> None:
    r = capture_bronze("whoop", "recovery", "not-bytes", {}, bronze_root=str(tmp_path))
    assert r.outcome == CaptureOutcome.FAILED and r.path is None


# @spec CORE-BRONZE-013
def test_request_scope_compares_only_with_the_same_request(tmp_path) -> None:
    same = b'{"laps":[]}'
    a = _cap(tmp_path, same, {"activity_id": 1}, dedupe_scope="request")
    b = _cap(tmp_path, same, {"activity_id": 2}, dedupe_scope="request")  # different request
    c = _cap(tmp_path, same, {"activity_id": 1}, dedupe_scope="request")  # re-capture of 1
    assert a.outcome == b.outcome == CaptureOutcome.WRITTEN
    assert c.outcome == CaptureOutcome.SKIPPED_IDENTICAL
    assert len(_payloads(tmp_path)) == 2


# @spec CORE-BRONZE-013
def test_request_scope_writes_a_changed_payload_and_ignores_key_order(tmp_path) -> None:
    _cap(tmp_path, b'{"v":1}', {"activity_id": 1, "format": "ORIGINAL"}, dedupe_scope="request")
    skipped = _cap(tmp_path, b'{"v":1}', {"format": "ORIGINAL", "activity_id": 1},
                   dedupe_scope="request")
    changed = _cap(tmp_path, b'{"v":2}', {"activity_id": 1, "format": "ORIGINAL"},
                   dedupe_scope="request")
    assert skipped.outcome == CaptureOutcome.SKIPPED_IDENTICAL
    assert changed.outcome == CaptureOutcome.WRITTEN


# @spec CORE-BRONZE-013
def test_request_scope_compares_with_the_newest_capture_of_that_request(tmp_path) -> None:
    _cap(tmp_path, b'{"v":1}', {"activity_id": 1}, dedupe_scope="request")
    _cap(tmp_path, b'{"v":2}', {"activity_id": 1}, dedupe_scope="request")
    back = _cap(tmp_path, b'{"v":1}', {"activity_id": 1}, dedupe_scope="request")
    assert back.outcome == CaptureOutcome.WRITTEN  # differs from the newest (v2)


# @spec CORE-BRONZE-009
def test_folder_scope_still_compares_with_the_folders_newest(tmp_path) -> None:
    _cap(tmp_path, b'{"x":1}', {"activity_id": 1})
    r = _cap(tmp_path, b'{"x":1}', {"activity_id": 2})  # folder scope: same bytes skipped
    assert r.outcome == CaptureOutcome.SKIPPED_IDENTICAL


# @spec CORE-BRONZE-014
def test_failed_sidecar_write_rolls_back_the_payload(tmp_path, monkeypatch) -> None:
    real = capture_mod._write_atomic

    def fail_sidecar(path, data):
        if path.endswith(".meta.json"):
            raise OSError("disk full")
        return real(path, data)

    monkeypatch.setattr(capture_mod, "_write_atomic", fail_sidecar)
    r = _cap(tmp_path, b'{"a":1}')
    assert r.outcome == CaptureOutcome.FAILED and r.path is None
    assert [f for f in _files(tmp_path) if os.path.isfile(f)] == []


# @spec CORE-BRONZE-015
def test_name_collision_retries_with_a_new_id(tmp_path, monkeypatch) -> None:
    ids = iter(["aaaaaa", "aaaaaa", "bbbbbb"])
    monkeypatch.setattr(
        capture_mod.secrets, "token_hex", lambda n: next(ids) if n == 3 else "f" * (2 * n)
    )
    monkeypatch.setattr(capture_mod, "_utc_now_ms", lambda: 1_790_000_000_000)
    first = _cap(tmp_path, b'{"a":1}', dedupe=False)
    second = _cap(tmp_path, b'{"a":2}', dedupe=False)
    assert first.outcome == second.outcome == CaptureOutcome.WRITTEN
    assert first.path != second.path and "bbbbbb" in second.path
    with open(first.path, "rb") as fh:
        assert fh.read() == b'{"a":1}'  # never overwritten


# @spec CORE-BRONZE-015
def test_persistent_collision_reports_failed_without_overwriting(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(capture_mod.secrets, "token_hex", lambda n: "c" * (2 * n))
    monkeypatch.setattr(capture_mod, "_utc_now_ms", lambda: 1_790_000_000_000)
    first = _cap(tmp_path, b'{"a":1}', dedupe=False)
    second = _cap(tmp_path, b'{"a":2}', dedupe=False)
    assert first.outcome == CaptureOutcome.WRITTEN
    assert second.outcome == CaptureOutcome.FAILED
    with open(first.path, "rb") as fh:
        assert fh.read() == b'{"a":1}'
