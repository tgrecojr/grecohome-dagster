"""Tests for the promote logic: idempotency, crash recovery, distinctness, junk."""

import glob
import os
from datetime import UTC, datetime
from pathlib import Path

import pytest
from grecohome_location.capture import capture_location
from grecohome_location.promote import (
    load_promoted_set,
    parse_received_ms,
    promote_stream,
    scan_staging,
    unpromoted_staging,
    window_dates,
)

NOW = datetime(2026, 7, 7, 12, 0, 0, tzinfo=UTC)
DT = "2026-07-07"
BODY = b'{"locations":[{"type":"Feature","properties":{"timestamp":"2026-07-07T12:00:00Z"}}]}'
OT = b'{"_type":"location","lat":1.0,"lon":2.0,"tst":123}'


def _stage(capture_dir, stream, dt, received_ms, shortid, body=BODY) -> str:
    d = Path(capture_dir) / stream / f"dt={dt}"
    d.mkdir(parents=True, exist_ok=True)
    name = f"{received_ms}_{shortid}.json"
    (d / name).write_bytes(body)
    return name


def _promote(capture_dir, bronze, state, stream, now=NOW):
    return promote_stream(
        capture_dir=capture_dir,
        bronze_root=bronze,
        state_dir=state,
        stream=stream,
        now=now,
        window_days=3,
    )


def _bronze_payloads(bronze, stream, dt=DT):
    pdir = os.path.join(bronze, "location", stream, f"dt={dt}")
    return [p for p in glob.glob(os.path.join(pdir, "*")) if not p.endswith(".meta.json")]


@pytest.mark.unit
class TestPromoteBasics:
    def test_overland_promote(self, tmp_path):
        cap, bronze, state = _dirs(tmp_path)
        name = _stage(cap, "overland", DT, 1751894460456, "d4e5f6")
        report = _promote(cap, bronze, state, "overland")

        assert report.promoted == 1
        assert report.bytes_promoted == len(BODY)
        payloads = _bronze_payloads(bronze, "overland")
        assert len(payloads) == 1
        with open(payloads[0], "rb") as fh:
            assert fh.read() == BODY
        assert name in load_promoted_set(state, "overland")

    def test_owntracks_promote(self, tmp_path):
        cap, bronze, state = _dirs(tmp_path)
        _stage(cap, "owntracks", DT, 1751894460456, "a1b2c3", body=OT)
        report = _promote(cap, bronze, state, "owntracks")
        assert report.promoted == 1
        payloads = _bronze_payloads(bronze, "owntracks")
        assert len(payloads) == 1
        with open(payloads[0], "rb") as fh:
            assert fh.read() == OT


@pytest.mark.unit
class TestIdempotency:
    def test_rerun_promotes_nothing_new(self, tmp_path):
        cap, bronze, state = _dirs(tmp_path)
        _stage(cap, "overland", DT, 1751894460456, "d4e5f6")
        first = _promote(cap, bronze, state, "overland")
        second = _promote(cap, bronze, state, "overland")
        assert first.promoted == 1
        assert second.promoted == 0
        assert second.already == 1
        assert len(_bronze_payloads(bronze, "overland")) == 1

    def test_crash_recovery_via_sidecar_backstop(self, tmp_path):
        """Bronze written but promoted-set never advanced -> no duplicate on re-run."""
        cap, bronze, state = _dirs(tmp_path)
        name = _stage(cap, "overland", DT, 1751894460456, "d4e5f6")
        # Simulate a prior run that wrote bronze then crashed before saving state.
        capture_location(
            BODY, collection="overland", dt=DT, received_ms=1751894460456,
            staging_file=name, bronze_root=bronze,
        )
        assert load_promoted_set(state, "overland") == set()  # state genuinely lost
        assert len(_bronze_payloads(bronze, "overland")) == 1

        report = _promote(cap, bronze, state, "overland")
        assert report.promoted == 0  # backstop recognised it as already promoted
        assert len(_bronze_payloads(bronze, "overland")) == 1  # no duplicate
        assert name in load_promoted_set(state, "overland")  # state rebuilt

    def test_byte_identical_distinct_posts_both_land(self, tmp_path):
        cap, bronze, state = _dirs(tmp_path)
        # Two DISTINCT staging files, identical bytes (e.g. a re-sent ping).
        _stage(cap, "owntracks", DT, 1751894460456, "aaaaaa", body=OT)
        _stage(cap, "owntracks", DT, 1751894460457, "bbbbbb", body=OT)
        report = _promote(cap, bronze, state, "owntracks")
        assert report.promoted == 2  # NOT collapsed by content dedup
        assert len(_bronze_payloads(bronze, "owntracks")) == 2


@pytest.mark.unit
class TestJunkAndParsing:
    def test_ignores_tmp_and_nonmatching(self, tmp_path):
        cap, bronze, state = _dirs(tmp_path)
        d = Path(cap) / "overland" / f"dt={DT}"
        d.mkdir(parents=True)
        (d / ".tmp_deadbeef").write_bytes(BODY)  # transient temp file
        (d / "notes.json").write_bytes(BODY)  # non-matching name
        (d / "123_ABCDEF.json").write_bytes(BODY)  # uppercase hex (relay uses lower)
        good = _stage(cap, "overland", DT, 1751894460456, "d4e5f6")
        report = _promote(cap, bronze, state, "overland")
        assert report.promoted == 1
        assert load_promoted_set(state, "overland") == {good}

    def test_parse_received_ms(self):
        assert parse_received_ms("1751894460456_d4e5f6.json") == 1751894460456
        assert parse_received_ms(".tmp_deadbeef") is None
        assert parse_received_ms("123_ABCDEF.json") is None
        assert parse_received_ms("foo.json") is None

    def test_window_dates(self):
        dates = window_dates(NOW, 3)
        assert dates == ["2026-07-05", "2026-07-06", "2026-07-07"]
        assert window_dates(NOW, 0) == ["2026-07-07"]  # clamped to >=1


@pytest.mark.unit
class TestScanDiagnostics:
    """A scanned=0 run must be self-explaining: missing vs denied vs empty."""

    def test_missing_root(self, tmp_path):
        cap, _, _ = _dirs(tmp_path)  # relay dir never created
        scan = scan_staging(cap, "overland", [DT])
        assert scan.root_status == "missing"
        assert scan.files == []
        assert scan.partitions_present == 0

    def test_ok_but_empty_window(self, tmp_path):
        cap, _, _ = _dirs(tmp_path)
        (Path(cap) / "overland" / f"dt={DT}").mkdir(parents=True)  # present but empty
        scan = scan_staging(cap, "overland", [DT])
        assert scan.root_status == "ok"
        assert scan.partitions_present == 1
        assert scan.files == []

    def test_permission_denied(self, tmp_path, monkeypatch):
        cap, _, _ = _dirs(tmp_path)
        (Path(cap) / "overland").mkdir(parents=True)
        real = os.listdir

        def fake(path):
            if str(path).startswith(cap):
                raise PermissionError(13, "Permission denied")
            return real(path)

        monkeypatch.setattr("grecohome_location.promote.os.listdir", fake)
        scan = scan_staging(cap, "overland", [DT])
        assert scan.root_status == "denied"  # loud signal: container not uid 1000
        assert scan.files == []
        assert scan.unreadable == 1

    def test_promote_report_carries_root_status(self, tmp_path):
        cap, bronze, state = _dirs(tmp_path)  # relay dir never created
        report = _promote(cap, bronze, state, "owntracks")
        assert report.scanned == 0
        assert report.root_status == "missing"
        assert report.partitions_present == 0


@pytest.mark.unit
class TestUnpromotedStaging:
    def test_reports_outstanding_then_empty_after_promote(self, tmp_path):
        cap, bronze, state = _dirs(tmp_path)
        _stage(cap, "overland", DT, 1751894460456, "d4e5f6")
        todo = unpromoted_staging(
            capture_dir=cap, bronze_root=bronze, state_dir=state,
            stream="overland", now=NOW, window_days=3,
        )
        assert len(todo) == 1
        _promote(cap, bronze, state, "overland")
        todo_after = unpromoted_staging(
            capture_dir=cap, bronze_root=bronze, state_dir=state,
            stream="overland", now=NOW, window_days=3,
        )
        assert todo_after == []


def _dirs(tmp_path):
    return (
        str(tmp_path / "relay"),
        str(tmp_path / "bronze"),
        str(tmp_path / "state"),
    )
