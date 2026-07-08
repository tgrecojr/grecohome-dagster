"""Call the self-hosted Photon ``/reverse`` endpoint.

Photon is a self-hosted OpenStreetMap reverse geocoder (``tuszik/photon-docker``,
upstream ``komoot/photon``). ``GET {base}/reverse?lat=..&lon=..`` returns a GeoJSON
``FeatureCollection`` of the nearest OSM object(s). No auth, no secret — it's a LAN
service — but we still keep a small tenacity retry so a transient restart doesn't fail a
whole run, and we return the response **bytes** verbatim so bronze stores them raw.
"""

from __future__ import annotations

import httpx
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from grecohome_core.logging_config import get_logger
from grecohome_geocode import __version__

log = get_logger(__name__)

_USER_AGENT = f"grecohome-geocode/{__version__} (+https://github.com/tgrecojr/grecohome-dagster)"

# Retry only transient transport failures (a Photon container restart / network blip);
# an HTTP error status is surfaced to the caller, which skips just that one cell.
_RETRYABLE = (httpx.TimeoutException, httpx.NetworkError)


def reverse_url(base_url: str) -> str:
    """Build the ``/reverse`` URL from the configured Photon base (no ``/api`` suffix)."""
    return base_url.rstrip("/") + "/reverse"


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=8),
    retry=retry_if_exception_type(_RETRYABLE),
    reraise=True,
)
def reverse_geocode(
    lat: float,
    lon: float,
    *,
    base_url: str,
    timeout: float = 30.0,
    language: str = "en",
    radius_km: float | None = 0.05,
) -> bytes:
    """Reverse-geocode a coordinate; return the raw GeoJSON response **bytes**.

    Raises ``httpx.HTTPStatusError`` on a non-2xx response (the caller leaves that cell
    un-cached and retries it next run). Transient transport errors are retried.
    """
    params: dict[str, object] = {"lat": lat, "lon": lon, "lang": language}
    if radius_km:
        params["radius"] = radius_km
    with httpx.Client(timeout=timeout, headers={"User-Agent": _USER_AGENT}) as client:
        resp = client.get(reverse_url(base_url), params=params)
    resp.raise_for_status()
    return resp.content
