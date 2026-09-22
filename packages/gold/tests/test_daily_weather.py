"""Transform-level tests for the gold daily weather mart."""

from __future__ import annotations

import os

import pytest

from grecohome_core.silver import connect
from grecohome_gold.daily_weather import MIN_VALID_HOURS, daily_weather_sql

pytestmark = pytest.mark.unit


def _rows(silver_root: str, min_valid_hours: int = 1) -> dict[str, dict]:
    """Rows keyed by day. The shared fixture has two obs per day, so these tests
    lower the completeness gate to 1; the gate itself is exercised below."""
    con = connect()
    cur = con.execute(daily_weather_sql(silver_root, min_valid_hours=min_valid_hours))
    cols = [d[0] for d in cur.description]
    return {
        r[cols.index("day")].isoformat(): dict(zip(cols, r, strict=True)) for r in cur.fetchall()
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
    assert r["air_temp_hours"] == 2 and r["soil_temp_5_hours"] == 2
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
    assert r["air_temp_hours"] == 0 and r["soil_moisture_50_hours"] == 0


# --- completeness gate ---------------------------------------------------------------


def _copy(con, select_sql: str, dest: str) -> None:
    os.makedirs(os.path.dirname(dest), exist_ok=True)
    con.execute(f"COPY ({select_sql}) TO '{dest}' (FORMAT PARQUET)")


@pytest.fixture
def gappy_silver_root(tmp_path) -> str:
    """A 24-hour local day where air temp is complete but the 5 cm soil probe drops out
    for the 7 afternoon hours (17 valid), the 50 cm probe is dead all day (0 valid), and
    two hours are NOAA "no transmission" rows (every measurement NULL)."""
    root = str(tmp_path / "silver")
    _copy(
        connect(),
        """
        SELECT
            DATE '2026-08-21' AS obs_date_local,
            CASE WHEN h IN (3, 4) THEN NULL ELSE 20.0 + h * 0.1 END AS air_temp_c,
            CASE WHEN h IN (3, 4) THEN NULL ELSE 21.0 + h * 0.1 END AS air_temp_max_c,
            CASE WHEN h IN (3, 4) THEN NULL ELSE 19.0 + h * 0.1 END AS air_temp_min_c,
            CASE WHEN h IN (3, 4) THEN NULL ELSE 0.0 END AS precip_mm,
            CASE WHEN h IN (3, 4) THEN NULL ELSE 100.0 END AS solar_rad_wm2,
            CASE WHEN h IN (3, 4) THEN NULL ELSE 25.0 END AS surface_temp_max_c,
            CASE WHEN h IN (3, 4) THEN NULL ELSE 15.0 END AS surface_temp_min_c,
            CASE WHEN h IN (3, 4) THEN NULL ELSE 60.0 END AS rh_pct,
            CASE WHEN h IN (3, 4) OR h BETWEEN 13 AND 19 THEN NULL ELSE 22.0 END AS soil_temp_5,
            CASE WHEN h IN (3, 4) THEN NULL ELSE 22.0 END AS soil_temp_10,
            CASE WHEN h IN (3, 4) THEN NULL ELSE 22.0 END AS soil_temp_20,
            CASE WHEN h IN (3, 4) THEN NULL ELSE 22.0 END AS soil_temp_50,
            CASE WHEN h IN (3, 4) THEN NULL ELSE 22.0 END AS soil_temp_100,
            CASE WHEN h IN (3, 4) THEN NULL ELSE 0.3 END AS soil_moisture_5,
            CASE WHEN h IN (3, 4) THEN NULL ELSE 0.3 END AS soil_moisture_10,
            CASE WHEN h IN (3, 4) THEN NULL ELSE 0.3 END AS soil_moisture_20,
            NULL::DOUBLE AS soil_moisture_50,
            CASE WHEN h IN (3, 4) THEN NULL ELSE 0.3 END AS soil_moisture_100
        FROM range(24) t(h)
    """,
        os.path.join(root, "weather", "silver_weather.parquet"),
    )
    return root


def test_default_gate_is_ncei_like() -> None:
    assert MIN_VALID_HOURS == 22


def test_gate_nulls_incomplete_fields_but_keeps_counts(gappy_silver_root: str) -> None:
    r = _rows(gappy_silver_root, min_valid_hours=MIN_VALID_HOURS)["2026-08-21"]
    # Two whole-row-null hours are not observations.
    assert r["hours_observed"] == 22 and r["has_weather"] is True
    # Air temp: 22 valid hours -> reported (with its extremes and GDD).
    assert r["air_temp_hours"] == 22
    assert r["air_temp_max_f"] is not None and r["gdd50"] is not None and r["frost"] is False
    # 5 cm soil temp: 15 valid hours -> gated to NULL, count explains why.
    assert r["soil_temp_5_hours"] == 15 and r["soil_temp_5_f_mean"] is None
    # Other depths complete -> reported.
    assert r["soil_temp_10_hours"] == 22 and r["soil_temp_10_f_mean"] == pytest.approx(71.6)
    # Dead 50 cm moisture probe -> 0 hours, NULL.
    assert r["soil_moisture_50_hours"] == 0 and r["soil_moisture_50_mean"] is None
    assert r["soil_moisture_5_mean"] == pytest.approx(0.3)


def test_lowering_the_gate_reports_partial_days(gappy_silver_root: str) -> None:
    r = _rows(gappy_silver_root, min_valid_hours=12)["2026-08-21"]
    assert r["soil_temp_5_f_mean"] == pytest.approx(71.6)  # avg over the 15 valid hours
    assert r["soil_moisture_50_mean"] is None  # still nothing to average
