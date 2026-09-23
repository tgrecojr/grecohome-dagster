"""Transform-level tests for silver strain over a synthetic Whoop cycle tree."""

from __future__ import annotations

import json
import os

import pytest

from grecohome_core.silver import connect, list_payload_files
from grecohome_silver.strain import bronze_strain_count_sql, strain_sql

pytestmark = pytest.mark.unit


def _cycle(cid, start, updated_at, *, offset="-04:00", strain=12.5, kj=8000.0,
           avg_hr=120, max_hr=170, state="SCORED", scored=True, steps=None) -> dict:
    rec = {
        "id": cid, "start": start, "end": start.replace("T1", "T2"),
        "timezone_offset": offset, "created_at": updated_at, "updated_at": updated_at,
        "score_state": state,
    }
    if steps is not None:  # Whoop added top-level ``step_count`` on 2026-09-23
        rec["step_count"] = steps
    if scored:
        rec["score"] = {"strain": strain, "kilojoule": kj,
                        "average_heart_rate": avg_hr, "max_heart_rate": max_hr}
    return rec


def _write(root: str, dt: str, fetched_ms: int, records: list[dict], short: str = "aa") -> None:
    pdir = os.path.join(root, "whoop", "cycle", f"dt={dt}")
    os.makedirs(pdir, exist_ok=True)
    with open(os.path.join(pdir, f"cycle_{fetched_ms}_{short}.json"), "w") as fh:
        json.dump({"records": records}, fh)
    with open(os.path.join(pdir, f"cycle_{fetched_ms}_{short}.meta.json"), "w") as fh:
        fh.write('{"sha256": "x"}')


def _rows(root: str) -> list[dict]:
    files = list_payload_files(root, "whoop", "cycle")
    con = connect()
    cur = con.execute(strain_sql(files))
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r, strict=True)) for r in cur.fetchall()]


def test_typing_and_local_start_date(tmp_path) -> None:
    root = str(tmp_path / "bronze")
    _write(root, "2026-06-10", 1_700_000_000000, [
        _cycle(1001, "2026-06-10T11:00:00.000Z", "2026-06-10T20:00:00.000Z"),
    ])
    (r,) = _rows(root)
    assert r["cycle_id"] == 1001
    assert r["strain_date"].isoformat() == "2026-06-10"  # 11:00Z − 4h = 07:00 local
    assert r["day_strain"] == 12.5 and r["kilojoules"] == 8000.0
    assert r["avg_heart_rate"] == 120 and r["max_heart_rate"] == 170
    assert r["step_count"] is None  # pre-2026-09-23 payloads have no step_count


def test_step_count_typed_when_present(tmp_path) -> None:
    """Whoop's ``step_count`` (added 2026-09-23) lands as a nullable INTEGER."""
    root = str(tmp_path / "bronze")
    _write(root, "2026-09-23", 1_790_191_146606, [
        _cycle(1817987567, "2026-09-23T01:41:40.820Z", "2026-09-23T19:00:00.000Z", steps=3256),
        _cycle(1815757587, "2026-09-22T01:37:30.260Z", "2026-09-23T19:00:00.000Z"),
    ])
    rows = {r["cycle_id"]: r for r in _rows(root)}
    assert rows[1817987567]["step_count"] == 3256
    assert rows[1815757587]["step_count"] is None


def test_local_start_date_can_roll_back_a_day(tmp_path) -> None:
    """An early-UTC start maps to the previous local day."""
    root = str(tmp_path / "bronze")
    _write(root, "2026-06-10", 1_700_000_000000, [
        _cycle(1002, "2026-06-10T02:00:00.000Z", "2026-06-10T20:00:00.000Z"),
    ])
    (r,) = _rows(root)
    assert r["strain_date"].isoformat() == "2026-06-09"  # 02:00Z − 4h = 22:00 prev day


def test_dedup_keeps_latest_rescore(tmp_path) -> None:
    root = str(tmp_path / "bronze")
    _write(root, "2026-06-10", 1_700_000_000000,
           [_cycle(1003, "2026-06-10T11:00:00.000Z", "2026-06-10T12:00:00.000Z", strain=10.0)],
           short="v1")
    _write(root, "2026-06-10", 1_700_000_999000,
           [_cycle(1003, "2026-06-10T11:00:00.000Z", "2026-06-10T22:00:00.000Z", strain=15.0)],
           short="v2")
    rows = _rows(root)
    assert len(rows) == 1 and rows[0]["day_strain"] == 15.0


def test_unscored_cycle_kept_with_null_metrics(tmp_path) -> None:
    root = str(tmp_path / "bronze")
    _write(root, "2026-06-10", 1_700_000_000000, [
        _cycle(1004, "2026-06-10T11:00:00.000Z", "2026-06-10T20:00:00.000Z",
               state="PENDING_SCORE", scored=False),
    ])
    (r,) = _rows(root)
    assert r["cycle_id"] == 1004 and r["day_strain"] is None
    assert r["score_state"] == "PENDING_SCORE"


def test_bronze_count_and_empty(tmp_path) -> None:
    root = str(tmp_path / "bronze")
    assert _rows(root) == []
    _write(root, "2026-06-10", 1_700_000_000000, [
        _cycle(1, "2026-06-10T11:00:00.000Z", "2026-06-10T20:00:00.000Z"),
        _cycle(2, "2026-06-11T11:00:00.000Z", "2026-06-11T20:00:00.000Z"),
    ])
    files = list_payload_files(root, "whoop", "cycle")
    assert int(connect().execute(bronze_strain_count_sql(files)).fetchone()[0]) == 2
