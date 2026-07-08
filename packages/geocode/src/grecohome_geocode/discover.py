"""Discover which grid cells need a Photon lookup (pure stdlib, read-only).

Bronze subjects carry no query engine, so this reads JSON with the stdlib (like the
core bronze checks) rather than DuckDB:

* **observed cells** — snap every point in the ``location`` bronze streams over a trailing
  window (Overland ``geometry.coordinates`` = ``[lon, lat]``; OwnTracks flat ``lat``/
  ``lon``) to its ``(lat_e4, lon_e4)`` cell.
* **cached cells** — the ``(lat_e4, lon_e4)`` recorded in a geocode bronze sidecar whose
  ``params_key`` matches the *current* lookup params (scanned across all partitions). Keying
  on the params too means a cell cached long ago is never re-queried, yet bumping the
  radius/limit/language re-looks-up every cell cleanly (old-params sidecars no longer count
  as done).

The work list is ``observed − cached``, sorted for determinism. Nothing here writes,
moves, or deletes under any bronze root.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

from grecohome_core.checks.bronze_reads import (
    collection_dir,
    iter_payloads,
    list_partition_dirs,
    read_sidecar,
)
from grecohome_core.logging_config import get_logger
from grecohome_geocode.capture import COLLECTION as GEOCODE_COLLECTION
from grecohome_geocode.capture import SOURCE as GEOCODE_SOURCE
from grecohome_geocode.cells import snap_e4

log = get_logger(__name__)

LOCATION_SOURCE = "location"
OVERLAND = "overland"
OWNTRACKS = "owntracks"

Cell = tuple[int, int]


def _window_dates(now: datetime, scan_days: int) -> set[str]:
    """The trailing ``scan_days`` UTC dates (incl. today) as ``YYYY-MM-DD`` strings."""
    n = max(1, scan_days)
    return {(now - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(n)}


def _load_json(payload_path: str) -> object | None:
    try:
        with open(payload_path, "rb") as fh:
            return json.loads(fh.read() or b"null")
    except (OSError, json.JSONDecodeError):
        return None


def _as_float(value: object) -> float | None:
    if isinstance(value, bool):  # guard: bool is an int subclass
        return None
    if isinstance(value, (int, float)):
        return float(value)
    return None


def overland_cells(obj: object, out: set[Cell]) -> None:
    """Add every valid point in one Overland payload (``{"locations": [...]}``)."""
    if not isinstance(obj, dict):
        return
    for loc in obj.get("locations") or []:
        if not isinstance(loc, dict):
            continue
        coords = (loc.get("geometry") or {}).get("coordinates")
        if not isinstance(coords, list) or len(coords) < 2:
            continue
        lon, lat = _as_float(coords[0]), _as_float(coords[1])
        if lat is not None and lon is not None:
            out.add((snap_e4(lat), snap_e4(lon)))


def owntracks_cells(obj: object, out: set[Cell]) -> None:
    """Add the point in one OwnTracks message (only messages that carry ``lat``/``lon``)."""
    if not isinstance(obj, dict):
        return
    lat, lon = _as_float(obj.get("lat")), _as_float(obj.get("lon"))
    if lat is not None and lon is not None:
        out.add((snap_e4(lat), snap_e4(lon)))


def observed_cells(bronze_root: str, *, scan_days: int, now: datetime | None = None) -> set[Cell]:
    """Distinct cells seen in the location bronze streams over the trailing window."""
    now = now or datetime.now(UTC)
    dates = _window_dates(now, scan_days)
    out: set[Cell] = set()
    for stream, extract in ((OVERLAND, overland_cells), (OWNTRACKS, owntracks_cells)):
        coll = collection_dir(bronze_root, LOCATION_SOURCE, stream)
        for dt, pdir in list_partition_dirs(coll):
            if dt not in dates:
                continue
            for payload in iter_payloads(pdir):
                obj = _load_json(payload)
                if obj is not None:
                    extract(obj, out)
    return out


def cached_cells(bronze_root: str, *, params_key: str) -> set[Cell]:
    """Cells cached under ``params_key`` — from every matching geocode bronze sidecar.

    A sidecar counts only if its ``params_key`` equals the current one, so a radius/limit/
    language change leaves every cell "not cached" and it is re-looked-up. Sidecars written
    before ``params_key`` existed never match, so they too re-look-up (superseded).
    """
    coll = collection_dir(bronze_root, GEOCODE_SOURCE, GEOCODE_COLLECTION)
    out: set[Cell] = set()
    for _dt, pdir in list_partition_dirs(coll):
        for payload in iter_payloads(pdir):
            sc = read_sidecar(payload) or {}
            if sc.get("params_key") != params_key:
                continue
            lat_e4, lon_e4 = sc.get("lat_e4"), sc.get("lon_e4")
            if isinstance(lat_e4, int) and isinstance(lon_e4, int):
                out.add((lat_e4, lon_e4))
    return out


def new_cells(
    bronze_root: str, *, scan_days: int, params_key: str, now: datetime | None = None
) -> list[Cell]:
    """Observed-minus-cached cells to look up, sorted for a deterministic run order."""
    observed = observed_cells(bronze_root, scan_days=scan_days, now=now)
    cached = cached_cells(bronze_root, params_key=params_key)
    return sorted(observed - cached)
