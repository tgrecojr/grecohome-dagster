"""Transform-level tests for silver weather over a synthetic USCRN bronze tree."""

from __future__ import annotations

import os

import pytest

from grecohome_core.silver import connect, list_payload_files
from grecohome_silver.weather import bronze_obs_count_sql, weather_sql

pytestmark = pytest.mark.unit

# 1-based CRNH0203 field index for each name we set; everything else stays a sentinel.
_FIELD_POS = {
    "t_avg": 10, "t_max": 11, "t_min": 12, "precip": 13, "solar": 14,
    "sur": 21, "sur_max": 23, "sur_min": 25, "rh": 27,
    "sm5": 29, "sm10": 30, "sm20": 31, "sm50": 32, "sm100": 33,
    "st5": 34, "st10": 35, "st20": 36, "st50": 37, "st100": 38,
}  # fmt: skip
# 1-based QC-flag field index for each flagged measurement (good = "0").
_FLAG_POS = {"solar": 15, "sur": 22, "sur_max": 24, "sur_min": 26, "rh": 28}


def _uscrn_row(utc_date: str, utc_time: str, *, wbanno: str = "03761", **vals) -> str:
    """Build one 38-field whitespace USCRN line; unset measurements stay sentinels."""
    f = ["-9999.0"] * 38
    f[0] = wbanno
    f[1] = utc_date
    f[2] = utc_time
    f[3], f[4] = utc_date, utc_time  # LST placeholders (not parsed)
    f[5], f[6], f[7] = "2.623", "-75.79", "39.86"
    f[13] = "-99999.0"  # SOLARAD sentinel
    for i in range(28, 33):  # soil-moisture sentinels (0-based 28..32 = fields 29..33)
        f[i] = "-99.0"
    for pos in _FLAG_POS.values():  # valid QC flags by default
        f[pos - 1] = "0"
    for name, value in vals.items():
        f[_FIELD_POS[name] - 1] = str(value)
    return " ".join(f)


def _write(root: str, dt: str, fetched_ms: int, rows: list[str], short: str = "aa") -> None:
    pdir = os.path.join(root, "uscrn", "hourly", f"dt={dt}")
    os.makedirs(pdir, exist_ok=True)
    with open(os.path.join(pdir, f"hourly_{fetched_ms}_{short}.txt"), "w") as fh:
        fh.write("\n".join(rows) + "\n")
    # Sidecar that must be ignored by the reader.
    with open(os.path.join(pdir, f"hourly_{fetched_ms}_{short}.meta.json"), "w") as fh:
        fh.write('{"sha256": "x"}')


def _rows(root: str, tz: str = "America/New_York") -> list[dict]:
    files = list_payload_files(root, "uscrn", "hourly")
    con = connect()
    con.execute("LOAD icu")
    cur = con.execute(weather_sql(files, timezone=tz))
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r, strict=True)) for r in cur.fetchall()]


def test_typing_keeps_metric_units(tmp_path) -> None:
    """Values are typed but never unit-converted (silver stays canonical SI)."""
    root = str(tmp_path / "bronze")
    _write(root, "2026-06-13", 1_700_000_000000, [
        _uscrn_row("20260613", "1700", t_avg=20.5, t_max=22.0, t_min=18.0,
                   precip=2.5, solar=800.0, sur=25.0, rh=65.0, sm5=0.30, st5=19.0),
    ])
    (r,) = _rows(root)
    assert r["wbanno"] == "03761"
    assert r["air_temp_c"] == 20.5 and r["air_temp_max_c"] == 22.0
    assert r["precip_mm"] == 2.5 and r["solar_rad_wm2"] == 800.0
    assert r["soil_moisture_5"] == 0.30 and r["soil_temp_5"] == 19.0
    assert r["obs_date_utc"].isoformat() == "2026-06-13"
    assert r["obs_ts_utc"].isoformat() == "2026-06-13T17:00:00"


def test_local_day_is_dst_aware_and_can_differ_from_utc(tmp_path) -> None:
    """Local day derives from the UTC instant via the station tz; it can roll back a day."""
    root = str(tmp_path / "bronze")
    # 02:00 UTC in summer = 22:00 EDT the previous local day.
    _write(root, "2026-06-13", 1_700_000_000000, [_uscrn_row("20260613", "0200", t_avg=15.0)])
    # 02:00 UTC in winter = 21:00 EST the previous local day.
    _write(root, "2026-01-13", 1_700_000_000000, [_uscrn_row("20260113", "0200", t_avg=1.0)],
           short="ww")
    by_utc = {r["obs_ts_utc"].isoformat(): r for r in _rows(root)}
    summer = by_utc["2026-06-13T02:00:00"]
    assert summer["obs_date_utc"].isoformat() == "2026-06-13"
    assert summer["obs_ts_local"].isoformat() == "2026-06-12T22:00:00"
    assert summer["obs_date_local"].isoformat() == "2026-06-12"
    winter = by_utc["2026-01-13T02:00:00"]
    assert winter["obs_ts_local"].isoformat() == "2026-01-12T21:00:00"


def test_sentinels_become_null_but_row_is_kept(tmp_path) -> None:
    """A fully-sentinel observation keeps its keys but nulls every measurement."""
    root = str(tmp_path / "bronze")
    _write(root, "2026-06-13", 1_700_000_000000, [_uscrn_row("20260613", "0500")])
    (r,) = _rows(root)
    assert r["obs_ts_utc"] is not None and r["obs_date_local"] is not None
    for col in ("air_temp_c", "precip_mm", "solar_rad_wm2", "rh_pct",
                "soil_moisture_5", "soil_temp_100"):
        assert r[col] is None, col


def test_dedup_keeps_latest_capture(tmp_path) -> None:
    """The same observation re-captured in two files collapses to the latest fetch."""
    root = str(tmp_path / "bronze")
    _write(root, "2026-06-13", 1_700_000_000000,
           [_uscrn_row("20260613", "1700", t_avg=20.0)], short="early")
    _write(root, "2026-06-13", 1_700_000_999000,
           [_uscrn_row("20260613", "1700", t_avg=21.0)], short="late")
    rows = _rows(root)
    assert len(rows) == 1 and rows[0]["air_temp_c"] == 21.0


def test_nul_byte_corruption_is_stripped(tmp_path) -> None:
    """A NUL-padded line (seen on DST-transition rows) parses with a clean WBANNO."""
    root = str(tmp_path / "bronze")
    line = "\x00\x00\x00" + _uscrn_row("20260308", "0700", t_avg=3.0)
    _write(root, "2026-03-08", 1_700_000_000000, [line])
    (r,) = _rows(root)
    assert r["wbanno"] == "03761" and r["air_temp_c"] == 3.0


def test_bronze_obs_count_and_sidecars_excluded(tmp_path) -> None:
    root = str(tmp_path / "bronze")
    _write(root, "2026-06-13", 1_700_000_000000, [
        _uscrn_row("20260613", "1700", t_avg=20.0),
        _uscrn_row("20260613", "1800", t_avg=21.0),
    ])
    files = list_payload_files(root, "uscrn", "hourly")
    assert all(not f.endswith(".meta.json") for f in files)
    assert int(connect().execute(bronze_obs_count_sql(files)).fetchone()[0]) == 2


def test_empty_yields_no_rows(tmp_path) -> None:
    """A not-yet-captured collection produces zero rows, not an error."""
    assert _rows(str(tmp_path / "bronze")) == []
