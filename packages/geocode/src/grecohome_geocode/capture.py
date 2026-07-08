"""Geocode capture adapter over the shared core bronze writer.

Captures one Photon ``/reverse`` response (raw GeoJSON bytes) to bronze. One collection:
``geocode/reverse``. ``dt`` is the UTC *lookup* date (when we asked Photon), not an event
date — geocoding a cell is an on-demand action, so there is no source timeline.

The sidecar carries the **cell key** (``lat_e4``/``lon_e4``) and the exact query point.
That cell key is what makes the cache idempotent (discovery skips cells already recorded
in a sidecar) and what ``silver_location`` joins on — the raw Photon body itself has no
notion of our grid, so we record the key beside it.

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


def capture_reverse(
    raw_bytes: bytes,
    *,
    lat_e4: int,
    lon_e4: int,
    query_lat: float,
    query_lon: float,
    radius_km: float | None,
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
        radius_km: The Photon search radius used (recorded for provenance).
        language: The Photon ``lang`` used.
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
        },
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
