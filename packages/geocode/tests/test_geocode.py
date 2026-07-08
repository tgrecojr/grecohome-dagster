"""Tests for the geocode run orchestration (discover -> look up -> cache)."""

import json
import os
from datetime import UTC, datetime

import httpx
import pytest

from grecohome_geocode import fetch, geocode
from grecohome_geocode.cells import cell_key
from grecohome_geocode.discover import cached_cells

pytestmark = pytest.mark.unit

NOW = datetime(2026, 7, 7, 12, 0, tzinfo=UTC)
RESP = b'{"type":"FeatureCollection","features":[{"type":"Feature","properties":{"name":"X"}}]}'


def _overland(root: str, points: list[tuple[float, float]]) -> None:
    locs = [
        {"geometry": {"coordinates": [lon, lat]}, "properties": {"timestamp": "2026-07-07T12:00Z"}}
        for lat, lon in points
    ]
    p = os.path.join(root, "location", "overland", "dt=2026-07-07", "overland_1_a.json")
    os.makedirs(os.path.dirname(p), exist_ok=True)
    with open(p, "w") as fh:
        json.dump({"locations": locs}, fh)


def _run(root, **kw):
    defaults = dict(
        bronze_root=root,
        photon_base_url="http://photon:2322",
        scan_days=3,
        max_lookups=2000,
        timeout=30.0,
        language="en",
        radius_km=0.05,
        now=NOW,
    )
    defaults.update(kw)
    return geocode.geocode_cells(**defaults)


class TestGeocodeCells:
    def test_looks_up_and_caches_new_cells(self, tmp_path, monkeypatch):
        root = str(tmp_path / "bronze")
        _overland(root, [(39.8, -75.1), (39.9, -75.2)])
        monkeypatch.setattr(fetch, "reverse_geocode", lambda *a, **k: RESP)

        report = _run(root)
        assert report.new_cells == 2
        assert report.captured == 2
        assert report.failed == 0
        assert cached_cells(root) == {cell_key(39.8, -75.1), cell_key(39.9, -75.2)}

    def test_second_run_is_noop(self, tmp_path, monkeypatch):
        root = str(tmp_path / "bronze")
        _overland(root, [(39.8, -75.1)])
        monkeypatch.setattr(fetch, "reverse_geocode", lambda *a, **k: RESP)
        _run(root)
        report = _run(root)  # everything already cached
        assert report.new_cells == 0
        assert report.captured == 0

    def test_cap_limits_lookups(self, tmp_path, monkeypatch):
        root = str(tmp_path / "bronze")
        _overland(root, [(39.8, -75.1), (39.9, -75.2), (40.0, -75.3)])
        monkeypatch.setattr(fetch, "reverse_geocode", lambda *a, **k: RESP)
        report = _run(root, max_lookups=2)
        assert report.capped is True
        assert report.new_cells == 3
        assert report.looked_up == 2
        assert report.captured == 2

    def test_http_failure_skips_cell(self, tmp_path, monkeypatch):
        root = str(tmp_path / "bronze")
        _overland(root, [(39.8, -75.1)])

        def _boom(*a, **k):
            raise httpx.HTTPStatusError("500", request=None, response=httpx.Response(500))

        monkeypatch.setattr(fetch, "reverse_geocode", _boom)
        report = _run(root)
        assert report.failed == 1
        assert report.captured == 0
        assert cached_cells(root) == set()  # nothing cached -> retried next run
