"""Tests for cell discovery over a synthetic location + geocode bronze tree."""

import json
import os
from datetime import UTC, datetime

import pytest

from grecohome_geocode import discover
from grecohome_geocode.capture import capture_reverse
from grecohome_geocode.cells import cell_key

pytestmark = pytest.mark.unit

NOW = datetime(2026, 7, 7, 12, 0, tzinfo=UTC)


def _write(path: str, obj: dict) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as fh:
        json.dump(obj, fh)


def _overland(root, dt, points, name="overland_1_a.json"):
    """Write one Overland payload ([lon, lat] GeoJSON) for a receipt date."""
    locs = [
        {"geometry": {"type": "Point", "coordinates": [lon, lat]},
         "properties": {"timestamp": f"{dt}T12:00:00Z"}}
        for lat, lon in points
    ]
    _write(os.path.join(root, "location", "overland", f"dt={dt}", name), {"locations": locs})


def _owntracks(root, dt, lat, lon, name="owntracks_1_a.json"):
    _write(
        os.path.join(root, "location", "owntracks", f"dt={dt}", name),
        {"_type": "location", "lat": lat, "lon": lon, "tst": 1751894460, "acc": 5},
    )


class TestObservedCells:
    def test_reads_both_streams(self, tmp_path):
        root = str(tmp_path / "bronze")
        _overland(root, "2026-07-07", [(39.8, -75.1), (39.9, -75.2)])
        _owntracks(root, "2026-07-07", 40.0, -75.3)
        cells = discover.observed_cells(root, scan_days=3, now=NOW)
        assert cells == {cell_key(39.8, -75.1), cell_key(39.9, -75.2), cell_key(40.0, -75.3)}

    def test_window_excludes_old_partitions(self, tmp_path):
        root = str(tmp_path / "bronze")
        _overland(root, "2026-07-07", [(39.8, -75.1)])
        _overland(root, "2026-01-01", [(10.0, 10.0)], name="overland_old_a.json")
        cells = discover.observed_cells(root, scan_days=3, now=NOW)
        assert cell_key(39.8, -75.1) in cells
        assert cell_key(10.0, 10.0) not in cells  # outside the 3-day window

    def test_owntracks_without_coords_skipped(self, tmp_path):
        root = str(tmp_path / "bronze")
        _write(
            os.path.join(root, "location", "owntracks", "dt=2026-07-07", "owntracks_lwt_a.json"),
            {"_type": "lwt", "tst": 1751894460},
        )
        assert discover.observed_cells(root, scan_days=3, now=NOW) == set()


KEY = "r=0.5;l=10;lang=en"


class TestCachedAndNew:
    def _cache(self, root, lat, lon, *, radius_km=0.5, limit=10, language="en"):
        la, lo = cell_key(lat, lon)
        capture_reverse(
            b'{"type":"FeatureCollection","features":[]}',
            lat_e4=la, lon_e4=lo, query_lat=lat, query_lon=lon,
            radius_km=radius_km, limit=limit, language=language,
            dt="2026-07-07", bronze_root=root,
        )

    def test_cached_cells_from_sidecars(self, tmp_path):
        root = str(tmp_path / "bronze")
        self._cache(root, 39.8, -75.1)
        assert discover.cached_cells(root, params_key=KEY) == {cell_key(39.8, -75.1)}

    def test_new_cells_is_observed_minus_cached(self, tmp_path):
        root = str(tmp_path / "bronze")
        _overland(root, "2026-07-07", [(39.8, -75.1), (39.9, -75.2)])
        self._cache(root, 39.8, -75.1)  # first one already cached
        assert discover.new_cells(root, scan_days=3, params_key=KEY, now=NOW) == [
            cell_key(39.9, -75.2)
        ]

    def test_new_cells_empty_when_all_cached(self, tmp_path):
        root = str(tmp_path / "bronze")
        _overland(root, "2026-07-07", [(39.8, -75.1)])
        self._cache(root, 39.8, -75.1)
        assert discover.new_cells(root, scan_days=3, params_key=KEY, now=NOW) == []

    def test_params_change_reinvalidates_cache(self, tmp_path):
        """A cell cached under different params (e.g. old radius) is re-looked-up."""
        root = str(tmp_path / "bronze")
        _overland(root, "2026-07-07", [(39.8, -75.1)])
        self._cache(root, 39.8, -75.1, radius_km=0.05)  # cached under OLD 50 m radius
        # Under the current params it is NOT cached -> queued for re-lookup.
        assert discover.new_cells(root, scan_days=3, params_key=KEY, now=NOW) == [
            cell_key(39.8, -75.1)
        ]
        # ...but under its own (old) params_key it is cached.
        assert discover.cached_cells(root, params_key="r=0.05;l=10;lang=en") == {
            cell_key(39.8, -75.1)
        }
