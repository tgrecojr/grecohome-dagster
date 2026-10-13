"""Transform-level tests for silver recovery over a synthetic Whoop recovery tree."""

from __future__ import annotations

import json
import os

import duckdb
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


# @spec SILVER-CYCLES-RECOVERY-001, SILVER-CYCLES-RECOVERY-002
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


# @spec SILVER-CYCLES-RECOVERY-003
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


# @spec SILVER-CYCLES-RECOVERY-003
def test_recovery_dedup_one_per_cycle(tmp_path) -> None:
    root = str(tmp_path / "bronze")
    _write(root, "2026-01-15", 1_700_000_000000, [_recovery(1), _recovery(2), _recovery(3)])
    # same cycles re-captured in a later file
    _write(root, "2026-01-16", 1_700_000_100000, [_recovery(2), _recovery(3)], short="bb")
    rows = _rows(root)
    assert len(rows) == 3
    assert {r["cycle_id"] for r in rows} == {1, 2, 3}


# @spec SILVER-CYCLES-RECOVERY-005
def test_recovery_calibrating_flag(tmp_path) -> None:
    root = str(tmp_path / "bronze")
    _write(root, "2026-01-15", 1_700_000_000000, [_recovery(111, calibrating=True)])
    assert _rows(root)[0]["user_calibrating"] is True


# @spec SILVER-CYCLES-RECOVERY-001
def test_recovery_sidecars_excluded_and_count(tmp_path) -> None:
    root = str(tmp_path / "bronze")
    _write(root, "2026-01-15", 1_700_000_000000, [_recovery(1), _recovery(2)])
    files = list_payload_files(root, "whoop", "recovery")
    assert all(not f.endswith(".meta.json") for f in files)
    assert int(connect().execute(bronze_recovery_count_sql(files)).fetchone()[0]) == 2


# @spec SILVER-CYCLES-RECOVERY-001
def test_recovery_empty_yields_no_rows(tmp_path) -> None:
    assert _rows(str(tmp_path / "bronze")) == []


def _write_raw(root: str, payload) -> None:
    pdir = os.path.join(root, "whoop", "recovery", "dt=2026-01-15")
    os.makedirs(pdir, exist_ok=True)
    with open(os.path.join(pdir, "recovery_1700000000000_raw.json"), "w") as fh:
        json.dump(payload, fh)


# @spec SILVER-CYCLES-RECOVERY-001
def test_recovery_payload_without_records_contributes_nothing(tmp_path) -> None:
    root = str(tmp_path / "bronze")
    _write(root, "2026-01-15", 1_700_000_100000, [_recovery(1)])
    _write_raw(root, {"next_token": None})
    assert [r["cycle_id"] for r in _rows(root)] == [1]


# @spec SILVER-CYCLES-RECOVERY-008
@pytest.mark.parametrize("records", [{"cycle_id": 1}, "oops", 5])
def test_recovery_non_array_records_fails_the_rebuild(tmp_path, records) -> None:
    root = str(tmp_path / "bronze")
    _write(root, "2026-01-15", 1_700_000_100000, [_recovery(1)])
    _write_raw(root, {"records": records})
    with pytest.raises(duckdb.Error):
        _rows(root)


# @spec SILVER-CYCLES-RECOVERY-004
def test_recovery_missing_id_dropped_non_numeric_id_kept_null(tmp_path) -> None:
    root = str(tmp_path / "bronze")
    no_id = {k: v for k, v in _recovery(1).items() if k != "cycle_id"}
    _write(root, "2026-01-15", 1_700_000_100000,
           [_recovery(1), no_id, {**_recovery(2), "cycle_id": None},
            {**_recovery(3), "cycle_id": "abc"}])
    ids = sorted((r["cycle_id"] for r in _rows(root)), key=lambda x: (x is None, x))
    assert ids == [1, None]


# @spec SILVER-CYCLES-RECOVERY-005, SILVER-CYCLES-RECOVERY-006
def test_recovery_score_state_carried_and_unscored_is_null(tmp_path) -> None:
    root = str(tmp_path / "bronze")
    pending = {k: v for k, v in _recovery(2).items() if k != "score"}
    pending["score_state"] = "PENDING_SCORE"
    _write(root, "2026-01-15", 1_700_000_100000, [_recovery(1), pending])
    rows = {r["cycle_id"]: r for r in _rows(root)}
    assert rows[1]["score_state"] == "SCORED"
    assert rows[2]["score_state"] == "PENDING_SCORE"
    assert rows[2]["recovery_score"] is None and rows[2]["user_calibrating"] is False
