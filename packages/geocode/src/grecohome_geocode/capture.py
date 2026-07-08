"""Geocode capture adapter over the shared core bronze writer.

Captures one Photon ``/reverse`` response (raw GeoJSON bytes) to bronze. One collection:
``geocode/reverse``. ``dt`` is the UTC *lookup* date (when we asked Photon), not an event
date — geocoding a cell is an on-demand action, so there is no source timeline.

The sidecar carries the **cell key** (``lat_e4``/``lon_e4``), the exact query point, and a
**params signature** (``params_key`` — the radius/limit/language that shaped the result).
The cell key + params_key are what make the cache idempotent (discovery skips a cell only
if it's cached under the *same* params) and what ``silver_location`` joins on — the raw
Photon body itself has no notion of our grid, so we record the key beside it. Recording the
params means a radius/limit/language change re-looks-up cleanly instead of reusing a stale
answer.

``dedupe=False``: idempotency is keyed on the **cell**, never on content. Two *distinct*
cells legitimately return identical responses (e.g. both an empty
``{"...","features":[]}`` "no result"), so content-hash dedup would drop the second and
leave that cell un-cached — re-looked-up on every run, forever. Since discovery already
guarantees one lookup per new cell, each capture is a genuine new cell and must land.
"""

from __future__ import annotations

from grecohome_core.bronze import capture_bronze
from grecohome_geocode import __version__
from grecohome_geocode.cells import CELL_PRECISION

SOURCE = "geocode"
COLLECTION = "reverse"
PROCESSOR = "grecohome-geocode"


def params_signature(*, radius_km: float | None, limit: int, language: str) -> str:
    """Stable signature of the lookup params that shape the result (the cache identity).

    Recorded in each sidecar as ``params_key`` and compared by discovery: a cell counts as
    "already cached" only if a sidecar has the *same* signature, so changing the radius,
    limit, or language re-looks-up cleanly. (``distance_sort`` is always on, so it's not
    part of the signature.)
    """
    r = "none" if radius_km is None else f"{radius_km:g}"
    return f"r={r};l={limit};lang={language}"


def capture_reverse(
    raw_bytes: bytes,
    *,
    lat_e4: int,
    lon_e4: int,
    query_lat: float,
    query_lon: float,
    radius_km: float | None,
    limit: int,
    language: str,
    dt: str,
    bronze_root: str,
    processor_version: str = __version__,
) -> str | None:
    """Capture one Photon reverse-geocode response (raw bytes) into bronze.

    Args:
        raw_bytes: The verbatim Photon GeoJSON response body.
        lat_e4, lon_e4: The cell key this response answers (the cache/join key).
        query_lat, query_lon: The exact point queried (the cell centre).
        radius_km: The Photon search radius used (part of the cache params signature).
        limit: The Photon result limit used (part of the cache params signature).
        language: The Photon ``lang`` used (part of the cache params signature).
        dt: UTC lookup date ``"YYYY-MM-DD"`` (bronze ``dt`` partition).
        bronze_root: Root directory for bronze output.

    Returns:
        The bronze payload path, or ``None`` if the core writer deduped/failed.
    """
    meta = {
        "request_url": "/reverse",
        "request_params": {
            "lat": query_lat,
            "lon": query_lon,
            "lang": language,
            "radius": radius_km,
            "limit": limit,
        },
        "params_key": params_signature(radius_km=radius_km, limit=limit, language=language),
        "http_status": 200,  # only 2xx responses reach here (fetch raises otherwise)
        "content_type": "application/json",
        "charset": "utf-8",
        "content_encoding": "identity",
        "stored_encoding": "identity",
        "processor": PROCESSOR,
        "processor_version": processor_version,
        "capture_mode": "raw",  # byte-exact Photon body (not reserialized)
        # The cell key + resolution — the cache's idempotency key and silver's join key.
        "cell_precision": CELL_PRECISION,
        "lat_e4": lat_e4,
        "lon_e4": lon_e4,
        "query_lat": query_lat,
        "query_lon": query_lon,
    }
    return capture_bronze(
        SOURCE,
        COLLECTION,
        raw_bytes,
        meta,
        bronze_root=bronze_root,
        dt=dt,
        dedupe=False,
        ext="json",
    )
