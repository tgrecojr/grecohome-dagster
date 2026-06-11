"""Transform-level tests for silver recovery over a synthetic Whoop recovery tree."""

from __future__ import annotations

import json
import os

import pytest

from grecohome_core.silver import connect, list_payload_files
from grecohome_silver.recovery import bronze_recovery_count_sql, recovery_sql

pytestmark = pytest.mark.unit


def _recovery(cycle_id: int, *, sleep_id="s-1", created="2026-01-15T12:00:00.000Z",
              updated="2026-01-15T12:30:00.000Z", score=66.0, calibrating=False) -> dict:
    return {
        "cycle_id": cycle_id,
        "sleep_id": sleep_id,
        "created_at": created,
        "updated_at": updated,
        "score_state": "SCORED",
        "score": {
            "user_calibrating": calibrating,
            "recovery_score": score,
            "resting_heart_rate": 58.0,
            "hrv_rmssd_milli": 42.5,
            "spo2_percentage": 96.0,
            "skin_temp_celsius": 33.4,
        },
    }


def _write(root: str, dt: str, fetched_ms: int, records: list[dict], short="aa") -> None:
    pdir = os.path.join(root, "whoop", "recovery", f"dt={dt}")
    os.makedirs(pdir, exist_ok=True)
    with open(os.path.join(pdir, f"recovery_{fetched_ms}_{short}.json"), "w") as fh:
        json.dump({"records": records}, fh)
    with open(os.path.join(pdir, f"recovery_{fetched_ms}_{short}.meta.json"), "w") as fh:
        fh.write('{"sha256": "x"}')


def _rows(root: str) -> list[dict]:
    files = list_payload_files(root, "whoop", "recovery")
    con = connect()
    cur = con.execute(recovery_sql(files))
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r, strict=True)) for r in cur.fetchall()]


def test_recovery_typing_and_join_keys(tmp_path) -> None:
    root = str(tmp_path / "bronze")
    _write(root, "2026-01-15", 1_700_000_000000, [_recovery(111, sleep_id="sleep-abc")])
    (r,) = _rows(root)
    assert r["cycle_id"] == 111
    assert r["sleep_id"] == "sleep-abc"  # joins to silver_sleep_whoop.whoop_sleep_id
    assert r["recovery_date"].isoformat() == "2026-01-15"
    assert r["recovery_score"] == pytest.approx(66.0)
    assert r["resting_heart_rate"] == pytest.approx(58.0)
    assert r["hrv_rmssd_milli"] == pytest.approx(42.5)
    assert r["user_calibrating"] is False


def test_recovery_rescore_keeps_latest(tmp_path) -> None:
    """One cycle rescored across two files -> keep the latest updated_at."""
    root = str(tmp_path / "bronze")
    _write(root, "2026-01-15", 1_700_000_000000,
           [_recovery(111, updated="2026-01-15T12:00:00.000Z", score=40.0)])
    _write(root, "2026-01-15", 1_700_000_999000,
           [_recovery(111, updated="2026-01-15T20:00:00.000Z", score=72.0)], short="bb")
    rows = _rows(root)
    assert len(rows) == 1
    assert rows[0]["recovery_score"] == pytest.approx(72.0)


def test_recovery_dedup_one_per_cycle(tmp_path) -> None:
    root = str(tmp_path / "bronze")
    _write(root, "2026-01-15", 1_700_000_000000, [_recovery(1), _recovery(2), _recovery(3)])
    # same cycles re-captured in a later file
    _write(root, "2026-01-16", 1_700_000_100000, [_recovery(2), _recovery(3)], short="bb")
    rows = _rows(root)
    assert len(rows) == 3
    assert {r["cycle_id"] for r in rows} == {1, 2, 3}


def test_recovery_calibrating_flag(tmp_path) -> None:
    root = str(tmp_path / "bronze")
    _write(root, "2026-01-15", 1_700_000_000000, [_recovery(111, calibrating=True)])
    assert _rows(root)[0]["user_calibrating"] is True


def test_recovery_sidecars_excluded_and_count(tmp_path) -> None:
    root = str(tmp_path / "bronze")
    _write(root, "2026-01-15", 1_700_000_000000, [_recovery(1), _recovery(2)])
    files = list_payload_files(root, "whoop", "recovery")
    assert all(not f.endswith(".meta.json") for f in files)
    assert int(connect().execute(bronze_recovery_count_sql(files)).fetchone()[0]) == 2


def test_recovery_empty_yields_no_rows(tmp_path) -> None:
    assert _rows(str(tmp_path / "bronze")) == []
