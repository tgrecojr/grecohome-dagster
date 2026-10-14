"""Garmin capture adapter over the shared core bronze writer.

These helpers build the Garmin-specific sidecar fields (capture grade, redacted
fields) and call ``grecohome_core.bronze.capture_bronze`` with an explicit ``ext``.

``dedupe`` (the core skip-identical check) is a **required** argument: Garmin
re-captures each trailing day on every tick, and whether an identical re-capture
may be skipped depends on the capture path (see ``pull.py`` and
``docs/intent/garmin/capture/capture-design.md``), so every call site must decide.
"""

from typing import Any

from grecohome_core.bronze import CaptureResult, capture_bronze
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


# @spec GARMIN-CAPTURE-010, GARMIN-CAPTURE-011
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
    dedupe: bool,
    dedupe_scope: str = "folder",
) -> CaptureResult:
    """Screen (if asked), reserialize, and capture a parsed JSON return.

    Returns the core writer's :class:`CaptureResult`.
    """
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
        bronze_root=bronze_root, dt=dt, dedupe=dedupe, dedupe_scope=dedupe_scope, ext="json",
    )


# @spec GARMIN-CAPTURE-010
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
    dedupe: bool,
    dedupe_scope: str = "folder",
) -> CaptureResult:
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
        bronze_root=bronze_root, dt=dt, dedupe=dedupe, dedupe_scope=dedupe_scope, ext=ext,
    )
