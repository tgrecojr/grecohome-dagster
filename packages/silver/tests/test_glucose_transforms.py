"""Transform-level tests for silver glucose over a synthetic Lingo CSV tree."""

from __future__ import annotations

import os

import pytest

from grecohome_core.silver import connect, list_payload_files
from grecohome_silver.glucose import bronze_reading_count_sql, glucose_sql

pytestmark = pytest.mark.unit

_HEADER = "Time of Glucose Reading [T=(local time) +/- (time zone offset)],Measurement(mg/dL),dt"


def _write_csv(
    root: str, dt: str, fetched_ms: int, rows: list[tuple[str, str]], short: str = "aa"
) -> None:
    pdir = os.path.join(root, "lingo", "glucose", f"dt={dt}")
    os.makedirs(pdir, exist_ok=True)
    lines = [_HEADER] + [f"{ts},{mgdl},{dt}" for ts, mgdl in rows]
    with open(os.path.join(pdir, f"glucose_{fetched_ms}_{short}.csv"), "w") as fh:
        fh.write("\n".join(lines) + "\n")
    # A sidecar that must be ignored by the reader.
    with open(os.path.join(pdir, f"glucose_{fetched_ms}_{short}.meta.json"), "w") as fh:
        fh.write('{"sha256": "x"}')


def _rows(root: str) -> list[dict]:
    files = list_payload_files(root, "lingo", "glucose")
    con = connect()
    cur = con.execute(glucose_sql(files))
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r, strict=True)) for r in cur.fetchall()]


def test_glucose_typing_local_utc_offset(tmp_path) -> None:
    """Local wall-clock, local date, signed offset, and derived UTC instant."""
    root = str(tmp_path / "bronze")
    _write_csv(root, "2026-03-15", 1_700_000_000000, [("2026-03-15T23:30-04:00", "90")])
    (r,) = _rows(root)
    assert r["reading_ts_local"].isoformat() == "2026-03-15T23:30:00"
    assert r["reading_date"].isoformat() == "2026-03-15"
    assert r["tz_offset_minutes"] == -240
    assert r["mgdl"] == 90
    # UTC = local - offset = 23:30 - (-4h) = next day 03:30.
    assert r["reading_ts_utc"].isoformat() == "2026-03-16T03:30:00"


def test_glucose_dedup_on_utc_instant_across_offset_reprs(tmp_path) -> None:
    """The same instant written under two offset spellings collapses to one reading.

    08:47-04:00 and 07:47-05:00 are both 12:47 UTC. Latest capture's representation
    wins, so the kept local fields come from the later file (07:47, -05:00).
    """
    root = str(tmp_path / "bronze")
    _write_csv(root, "2026-03-15", 1_700_000_000000, [("2026-03-15T08:47-04:00", "120")])
    _write_csv(
        root, "2026-03-15", 1_700_000_999000, [("2026-03-15T07:47-05:00", "120")], short="bb"
    )
    rows = _rows(root)
    assert len(rows) == 1
    assert rows[0]["reading_ts_utc"].isoformat() == "2026-03-15T12:47:00"
    assert rows[0]["tz_offset_minutes"] == -300  # latest capture's representation
    assert rows[0]["reading_ts_local"].isoformat() == "2026-03-15T07:47:00"


def test_glucose_duplicate_captures_collapse(tmp_path) -> None:
    """The same reading re-exported in many files dedups to one row."""
    root = str(tmp_path / "bronze")
    for i, ms in enumerate((1_700_000_000000, 1_700_000_100000, 1_700_000_200000)):
        _write_csv(root, "2026-03-15", ms, [("2026-03-15T06:00-04:00", "100")], short=f"c{i}")
    rows = _rows(root)
    assert len(rows) == 1
    assert rows[0]["mgdl"] == 100


def test_glucose_keeps_null_measurement(tmp_path) -> None:
    """A reading with a blank measurement is kept with mgdl null (never dropped)."""
    root = str(tmp_path / "bronze")
    _write_csv(root, "2026-03-15", 1_700_000_000000, [("2026-03-15T06:00-04:00", "")])
    rows = _rows(root)
    assert len(rows) == 1
    assert rows[0]["mgdl"] is None
    assert rows[0]["reading_ts_utc"] is not None


def test_glucose_sidecars_excluded_and_count(tmp_path) -> None:
    """Sidecars are ignored; bronze_reading_count_sql counts distinct instants."""
    root = str(tmp_path / "bronze")
    _write_csv(root, "2026-03-15", 1_700_000_000000,
               [("2026-03-15T06:00-04:00", "100"), ("2026-03-15T06:05-04:00", "101")])
    files = list_payload_files(root, "lingo", "glucose")
    assert all(not f.endswith(".meta.json") for f in files)
    assert int(connect().execute(bronze_reading_count_sql(files)).fetchone()[0]) == 2


def test_glucose_empty_yields_no_rows(tmp_path) -> None:
    """A not-yet-captured collection produces zero rows, not an error."""
    root = str(tmp_path / "bronze")
    assert _rows(root) == []
