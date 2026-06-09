"""Lingo capture adapter over the shared core bronze writer.

Captures a downloaded glucose CSV's raw bytes (one collection: ``lingo/glucose``)
with Drive provenance in the sidecar. ``dedupe=True`` skips a byte-identical
re-upload; the per-file_id Dagster partition is the primary "capture once" guard.
Never records tokens/credentials.
"""

from grecohome_core.bronze import capture_bronze
from grecohome_lingo import __version__

SOURCE = "lingo"
COLLECTION = "glucose"
PROCESSOR = "grecohome-lingo"


def capture_glucose(
    raw_bytes: bytes,
    *,
    file_id: str,
    file_name: str,
    folder_id: str,
    bronze_root: str,
    created_time: str | None = None,
    modified_time: str | None = None,
    processor_version: str = __version__,
) -> str | None:
    """Capture a raw Lingo glucose CSV (bytes) to the bronze layer."""
    meta = {
        # Drive media URL — carries no token/auth, safe to record for replay.
        "request_url": f"https://www.googleapis.com/drive/v3/files/{file_id}?alt=media",
        "request_params": {
            "file_id": file_id,
            "file_name": file_name,
            "drive_folder_id": folder_id,
            "created_time": created_time,
            "modified_time": modified_time,
        },
        "http_status": 200,
        "content_type": "text/csv",  # -> ext "csv" via core's content-type map
        "charset": "utf-8",
        "content_encoding": "identity",
        "stored_encoding": "identity",
        "processor": PROCESSOR,
        "processor_version": processor_version,
    }
    # dt=None -> fetch/capture date (cumulative dumps have no single event date).
    return capture_bronze(SOURCE, COLLECTION, raw_bytes, meta, bronze_root=bronze_root, dedupe=True)
