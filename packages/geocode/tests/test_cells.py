"""Tests for the grid-cell key contract (must match DuckDB round() on the silver side)."""

import pytest

from grecohome_geocode.cells import CELL_PRECISION, cell_center, cell_key, snap_e4

pytestmark = pytest.mark.unit


class TestSnapE4:
    def test_basic_precision(self):
        # 4 decimals -> integer 1e-4 degrees.
        assert snap_e4(39.8) == 398000
        assert snap_e4(-75.1) == -751000

    def test_rounds_to_nearest_cell(self):
        assert snap_e4(39.80004) == 398000  # rounds down
        assert snap_e4(39.80006) == 398001  # rounds up

    def test_half_away_from_zero_matches_duckdb(self):
        # DuckDB round() rounds halves away from zero; snap_e4 must agree.
        assert snap_e4(39.800050) == 398001  # +0.5 -> away from zero (up)
        assert snap_e4(-39.800050) == -398001  # -0.5 -> away from zero (down)

    def test_precision_constant(self):
        assert CELL_PRECISION == 4


class TestCellCenterAndKey:
    def test_center_is_inverse_of_snap(self):
        assert cell_center(398000) == pytest.approx(39.8)
        assert cell_center(-751000) == pytest.approx(-75.1)

    def test_cell_key_pairs_lat_lon(self):
        assert cell_key(39.8, -75.1) == (398000, -751000)

    def test_round_trip_center_snaps_back(self):
        e4 = snap_e4(39.81234)
        assert snap_e4(cell_center(e4)) == e4
