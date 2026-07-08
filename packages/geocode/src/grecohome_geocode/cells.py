"""The grid-cell key — the one contract the bronze cache and ``silver_location`` share.

A coordinate is snapped to an integer index of 1e-4-degree cells (~11 m at the equator).
The cache is keyed by ``(lat_e4, lon_e4)``; ``silver_location`` recomputes the *same*
key in DuckDB (``CAST(round(coord * 10000) AS BIGINT)``) and LEFT JOINs on it. The two
must agree exactly, so:

* the scale (``10 ** CELL_PRECISION``) is fixed here, and
* :func:`snap_e4` rounds **half away from zero**, matching DuckDB's ``round()`` — an
  exact-half tie at the 5th decimal is astronomically rare given GPS noise, and a
  one-cell disagreement would only mean a cache miss for a neighbouring ~11 m cell
  (harmless: the point is simply re-looked-up), never a wrong address.

To change resolution later, bump :data:`CELL_PRECISION`, re-run the cache once with a
wide ``geocode_scan_days``, and rebuild ``silver_location`` — bronze keeps the raw
points and raw Photon responses, so nothing is lost.
"""

from __future__ import annotations

import math

#: Decimal degrees of precision in a cell key. 4 → ~11 m cells.
CELL_PRECISION = 4

#: Integer scale factor: a degree * this, rounded, is the cell index.
_SCALE = 10**CELL_PRECISION


def snap_e4(coord: float) -> int:
    """Snap a WGS84 degree to its integer 1e-4-degree cell index.

    Rounds half away from zero to match DuckDB's ``round()`` (used by the silver side),
    so the same coordinate produces the same key in both Python and SQL.
    """
    scaled = coord * _SCALE
    return math.floor(scaled + 0.5) if scaled >= 0 else math.ceil(scaled - 0.5)


def cell_center(e4: int) -> float:
    """The representative (centre) degree for a cell index — what we query Photon at."""
    return e4 / _SCALE


def cell_key(lat: float, lon: float) -> tuple[int, int]:
    """The ``(lat_e4, lon_e4)`` cache key for a coordinate."""
    return snap_e4(lat), snap_e4(lon)
