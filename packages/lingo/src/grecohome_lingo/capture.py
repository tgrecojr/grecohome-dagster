"""Lingo capture adapter over the shared core bronze writer.

Captures a downloaded glucose CSV's raw bytes (one collection: ``lingo/glucose``)
with Drive provenance in the sidecar. The per-file_id Dagster partition is the
"capture once" guard (each Drive file is captured exactly once), so cross-file
content dedup is intentionally **off**: bronze partitions by fetch date, not by
file_id, so two *distinct* Drive files that happen to be byte-identical and are
captured on the same day would otherwise collapse to one payload — silently
dropping the second file's bronze record while its partition is still marked
captured. Never records tokens/credentials.
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
    """Capture a raw Lingo glucose CSV (bytes) to the bronze layer.

    Returns the bronze payload path, or ``None`` only if the underlying capture
    raised (the core writer swallows capture errors). Identical content is **not**
    deduped — see the module docstring.
    """
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
    # dedupe=False: the per-file_id partition is the capture-once guard; content dedup
    # over a fetch-date partition could silently drop a distinct, byte-identical file.
    return capture_bronze(
        SOURCE, COLLECTION, raw_bytes, meta, bronze_root=bronze_root, dedupe=False
    )
