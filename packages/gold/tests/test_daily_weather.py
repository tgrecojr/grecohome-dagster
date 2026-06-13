"""Transform-level tests for the gold daily weather mart."""

from __future__ import annotations

import pytest

from grecohome_core.silver import connect
from grecohome_gold.daily_weather import daily_weather_sql

pytestmark = pytest.mark.unit


def _rows(silver_root: str) -> dict[str, dict]:
    con = connect()
    cur = con.execute(daily_weather_sql(silver_root))
    cols = [d[0] for d in cur.description]
    return {
        r[cols.index("day")].isoformat(): dict(zip(cols, r, strict=True))
        for r in cur.fetchall()
    }


def test_continuous_daily_spine(weather_silver_root: str) -> None:
    """One row per local day across the range, the 2026-04-21 gap included."""
    assert sorted(_rows(weather_silver_root)) == ["2026-04-20", "2026-04-21", "2026-04-22"]


def test_missing_silver_weather_yields_empty(tmp_path) -> None:
    """A not-yet-materialized silver_weather builds to zero rows rather than erroring."""
    assert _rows(str(tmp_path / "absent_silver")) == {}


def test_frost_day_aggregates_imperial(weather_silver_root: str) -> None:
    """Daily max/min in °F, frost vs hard-freeze flags, precip in inches."""
    r = _rows(weather_silver_root)["2026-04-20"]
    assert r["has_weather"] is True and r["hours_observed"] == 2
    assert r["air_temp_max_f"] == pytest.approx(46.4)  # max(8 °C)
    assert r["air_temp_min_f"] == pytest.approx(28.4)  # min(−2 °C)
    assert r["air_temp_avg_f"] == pytest.approx(32.9)  # avg(2, −1)=0.5 °C
    assert r["frost"] is True  # 28.4 ≤ 32
    assert r["hard_freeze"] is False  # 28.4 > 28
    assert r["gdd50"] == pytest.approx(0.0)  # (46.4+28.4)/2 = 37.4 < 50
    assert r["precip_total_in"] == pytest.approx(0.2)  # 5.08 mm / 25.4
    assert r["solar_rad_max_wm2"] == pytest.approx(300.0)
    assert r["surface_temp_min_f"] == pytest.approx(26.6)  # min(−3 °C)
    assert r["soil_temp_5_f_mean"] == pytest.approx(41.0)  # avg(4,6)=5 °C
    assert r["soil_moisture_5_mean"] == pytest.approx(0.40)


def test_warm_day_growing_degree_days(weather_silver_root: str) -> None:
    r = _rows(weather_silver_root)["2026-04-22"]
    assert r["frost"] is False
    assert r["air_temp_max_f"] == pytest.approx(82.4)  # max(28 °C)
    assert r["gdd50"] == pytest.approx(18.0)  # (82.4+53.6)/2 − 50
    assert r["rh_mean_pct"] == pytest.approx(55.0)
    assert r["soil_temp_5_f_mean"] == pytest.approx(66.2)  # avg(18,20)=19 °C


def test_gap_day_is_null(weather_silver_root: str) -> None:
    """A spine day with no observations: provenance false, aggregates null, 0 hours."""
    r = _rows(weather_silver_root)["2026-04-21"]
    assert r["has_weather"] is False and r["hours_observed"] == 0
    assert r["air_temp_max_f"] is None and r["gdd50"] is None and r["frost"] is None
