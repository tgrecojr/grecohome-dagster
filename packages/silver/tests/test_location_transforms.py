"""Transform-level tests for silver location over a synthetic bronze tree.

Builds a tiny ``location`` (Overland + OwnTracks) + ``geocode`` bronze tree and runs the
SQL directly, so the point normalization, cell-key snapping, and the sidecar-keyed
geocode join are all exercised without materializing an asset.
"""

from __future__ import annotations

import json
import os

import pytest

from grecohome_core.silver import connect, list_payload_files
from grecohome_silver.dagster.location_assets import list_sidecar_files
from grecohome_silver.location import bronze_point_count_sql, location_sql

pytestmark = pytest.mark.unit


def _write(path: str, obj: dict) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as fh:
        json.dump(obj, fh)


def _overland(root, dt, fetched_ms, points, short="a") -> None:
    """One Overland payload: points are (lat, lon[, accuracy]) — stored [lon, lat]."""
    locs = []
    for p in points:
        lat, lon = p[0], p[1]
        props = {"timestamp": f"{dt}T12:00:00Z"}
        if len(p) > 2:
            props["horizontal_accuracy"] = p[2]
        locs.append({"geometry": {"coordinates": [lon, lat]}, "properties": props})
    name = f"overland_{fetched_ms}_{short}.json"
    _write(os.path.join(root, "location", "overland", f"dt={dt}", name), {"locations": locs})


def _owntracks(root, dt, fetched_ms, lat, lon, tst, acc=None, typ="location", short="a") -> None:
    obj = {"_type": typ, "tst": tst}
    if lat is not None:
        obj["lat"] = lat
    if lon is not None:
        obj["lon"] = lon
    if acc is not None:
        obj["acc"] = acc
    name = f"owntracks_{fetched_ms}_{short}.json"
    _write(os.path.join(root, "location", "owntracks", f"dt={dt}", name), obj)


def _geocode(root, dt, fetched_ms, lat_e4, lon_e4, props, short="a") -> None:
    """A geocode cache entry: payload (Photon FeatureCollection) + sidecar (cell key)."""
    stem = os.path.join(root, "geocode", "reverse", f"dt={dt}", f"reverse_{fetched_ms}_{short}")
    os.makedirs(os.path.dirname(stem), exist_ok=True)
    features = [] if props is None else [{"type": "Feature", "properties": props}]
    with open(stem + ".json", "w") as fh:
        json.dump({"type": "FeatureCollection", "features": features}, fh)
    with open(stem + ".meta.json", "w") as fh:
        json.dump(
            {"source": "geocode", "collection": "reverse", "lat_e4": lat_e4,
             "lon_e4": lon_e4, "fetched_at_unix_ms": fetched_ms}, fh
        )


def _rows(root: str) -> list[dict]:
    ov = list_payload_files(root, "location", "overland")
    ot = list_payload_files(root, "location", "owntracks")
    gp = list_payload_files(root, "geocode", "reverse")
    gs = list_sidecar_files(root, "geocode", "reverse")
    con = connect()
    cur = con.execute(location_sql(ov, ot, gp, gs))
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r, strict=True)) for r in cur.fetchall()]


def test_overland_coordinates_and_cell_key(tmp_path) -> None:
    """Overland [lon, lat] parsed correctly; cell key = round(coord * 1e4)."""
    root = str(tmp_path / "bronze")
    _overland(root, "2026-07-07", 1_700_000_000000, [(39.8, -75.1, 5)])
    (r,) = _rows(root)
    assert r["source_stream"] == "overland"
    assert r["lat"] == 39.8 and r["lon"] == -75.1
    assert r["lat_e4"] == 398000 and r["lon_e4"] == -751000
    assert r["accuracy_m"] == 5
    assert r["event_ts_utc"].isoformat() == "2026-07-07T12:00:00"


def test_owntracks_tst_epoch_seconds(tmp_path) -> None:
    """OwnTracks flat lat/lon + tst (epoch seconds) -> UTC timestamp."""
    root = str(tmp_path / "bronze")
    _owntracks(root, "2026-07-07", 1_700_000_000000, 40.0, -75.3, 1751894460, acc=8)
    (r,) = _rows(root)
    assert r["source_stream"] == "owntracks"
    assert r["lat"] == 40.0 and r["lon"] == -75.3
    assert r["event_ts_utc"].isoformat() == "2025-07-07T13:21:00"


def test_owntracks_non_location_message_dropped(tmp_path) -> None:
    """A message without lat/lon (lwt/transition) is not a fix."""
    root = str(tmp_path / "bronze")
    _owntracks(root, "2026-07-07", 1_700_000_000000, None, None, 1751894461, typ="lwt")
    assert _rows(root) == []


def test_geocode_join_attaches_place(tmp_path) -> None:
    """A fix whose cell is cached gets place fields; an uncached one is geocoded=False."""
    root = str(tmp_path / "bronze")
    _overland(root, "2026-07-07", 1_700_000_000000, [(39.8, -75.1), (39.9, -75.2)])
    _geocode(root, "2026-07-07", 1_700_000_000000, 398000, -751000,
             {"name": "Home", "city": "Avondale", "state": "Pennsylvania",
              "osm_key": "place", "osm_value": "house", "osm_id": 42})
    rows = {(r["lat_e4"], r["lon_e4"]): r for r in _rows(root)}
    hit = rows[(398000, -751000)]
    assert hit["geocoded"] is True
    assert hit["geo_name"] == "Home"
    assert hit["geo_city"] == "Avondale"
    assert hit["geo_osm_value"] == "house"
    assert hit["geo_osm_id"] == 42
    miss = rows[(399000, -752000)]
    assert miss["geocoded"] is False
    assert miss["geo_name"] is None


def test_empty_photon_result_is_cached_but_unnamed(tmp_path) -> None:
    """A cell with an empty Photon result counts as geocoded but has no name."""
    root = str(tmp_path / "bronze")
    _overland(root, "2026-07-07", 1_700_000_000000, [(39.8, -75.1)])
    _geocode(root, "2026-07-07", 1_700_000_000000, 398000, -751000, None)  # features: []
    (r,) = _rows(root)
    assert r["geocoded"] is True
    assert r["geo_name"] is None


def test_geocode_dedup_latest_capture_wins(tmp_path) -> None:
    """Two cache captures of one cell: the latest (by fetched_ms) wins."""
    root = str(tmp_path / "bronze")
    _overland(root, "2026-07-07", 1_700_000_000000, [(39.8, -75.1)])
    _geocode(root, "2026-07-07", 1_700_000_000000, 398000, -751000, {"name": "Old"}, short="a")
    _geocode(root, "2026-07-08", 1_700_000_999000, 398000, -751000, {"name": "New"}, short="b")
    (r,) = _rows(root)
    assert r["geo_name"] == "New"


def test_duplicate_fix_collapses(tmp_path) -> None:
    """The same (stream, instant) re-promoted in two files dedups to one row."""
    root = str(tmp_path / "bronze")
    _overland(root, "2026-07-07", 1_700_000_000000, [(39.8, -75.1)], short="a")
    _overland(root, "2026-07-07", 1_700_000_100000, [(39.8, -75.1)], short="b")
    assert len(_rows(root)) == 1


def test_bronze_point_count(tmp_path) -> None:
    root = str(tmp_path / "bronze")
    _overland(root, "2026-07-07", 1_700_000_000000, [(39.8, -75.1), (39.9, -75.2)])
    _owntracks(root, "2026-07-07", 1_700_000_000000, 40.0, -75.3, 1751894460)
    ov = list_payload_files(root, "location", "overland")
    ot = list_payload_files(root, "location", "owntracks")
    assert int(connect().execute(bronze_point_count_sql(ov, ot)).fetchone()[0]) == 3


def test_empty_tree_yields_no_rows(tmp_path) -> None:
    root = str(tmp_path / "bronze")
    assert _rows(root) == []
