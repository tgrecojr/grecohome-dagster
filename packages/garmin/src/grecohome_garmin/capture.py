"""Garmin capture adapter over the shared core bronze writer.

garmincapture had its own ``capture_bronze``; in the monorepo there is one writer
(``grecohome_core.bronze.capture_bronze``). These helpers build the Garmin-specific
sidecar fields (capture grade, redacted fields) and call core with ``dedupe=False``
(Garmin data is immutable -- keep every capture) and an explicit ``ext``.
"""

from typing import Any

from grecohome_core.bronze import capture_bronze
from grecohome_garmin import __version__
from grecohome_garmin.serialize import screen_for_secrets, to_bronze_json

SOURCE = "garmin"
PROCESSOR = "grecohome-garmin"
MODE_RAW = "raw"
MODE_RESERIALIZED = "reserialized"

# Content-type map for binary download formats (raw grade).
DOWNLOAD_EXT: dict[str, tuple[str, str]] = {
    "ORIGINAL": ("zip", "application/zip"),
    "TCX": ("tcx", "application/vnd.garmin.tcx+xml"),
    "GPX": ("gpx", "application/gpx+xml"),
    "KML": ("kml", "application/vnd.google-earth.kml+xml"),
    "CSV": ("csv", "text/csv"),
}


def capture_json(
    collection: str,
    parsed: Any,
    *,
    request_url: str,
    request_params: dict,
    bronze_root: str,
    processor_version: str = __version__,
    dt: str | None = None,
    screen_secrets: bool = False,
) -> str | None:
    """Screen (if asked), reserialize, and capture a parsed JSON return."""
    redacted: list[str] = []
    payload = parsed
    if screen_secrets:
        payload, redacted = screen_for_secrets(parsed)
    meta = {
        "request_url": request_url,
        "request_params": request_params,
        "http_status": 200,
        "content_type": "application/json",
        "charset": "utf-8",
        "content_encoding": "identity",
        "stored_encoding": "identity",
        "capture_mode": MODE_RESERIALIZED,
        "redacted_fields": redacted,
        "processor": PROCESSOR,
        "processor_version": processor_version,
    }
    return capture_bronze(
        SOURCE, collection, to_bronze_json(payload), meta,
        bronze_root=bronze_root, dt=dt, dedupe=False, ext="json",
    )


def capture_raw(
    collection: str,
    raw_bytes: bytes,
    *,
    ext: str,
    content_type: str,
    request_url: str,
    request_params: dict,
    bronze_root: str,
    processor_version: str = __version__,
    dt: str | None = None,
) -> str | None:
    """Capture binary download bytes at the ``raw`` grade (true bytes, no reserialize)."""
    meta = {
        "request_url": request_url,
        "request_params": request_params,
        "http_status": 200,
        "content_type": content_type,
        "charset": None,
        "content_encoding": "identity",
        "stored_encoding": "identity",
        "capture_mode": MODE_RAW,
        "redacted_fields": [],
        "processor": PROCESSOR,
        "processor_version": processor_version,
    }
    return capture_bronze(
        SOURCE, collection, raw_bytes, meta,
        bronze_root=bronze_root, dt=dt, dedupe=False, ext=ext,
    )
