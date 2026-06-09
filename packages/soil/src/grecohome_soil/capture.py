"""Soil (USCRN) capture adapter over the shared core bronze writer.

Captures one UTC date's rows (one collection: ``uscrn/hourly``) as raw text,
sliced from the station's year file. ``dedupe=True`` skips a re-capture when the
day's rows are unchanged, so re-pulling a still-filling day costs ~zero storage.
Never records anything beyond public source provenance.
"""

from grecohome_core.bronze import capture_bronze
from grecohome_soil import __version__

SOURCE = "uscrn"
COLLECTION = "hourly"
PROCESSOR = "grecohome-soil"


def capture_hourly(
    rows: list[str],
    *,
    station: str,
    partition_date: str,
    year: int,
    source_url: str,
    bronze_root: str,
    processor_version: str = __version__,
) -> str | None:
    """Capture one UTC day's USCRN rows (raw text) to the bronze layer.

    Skips the write entirely when there are no rows for the date (e.g. early in the
    UTC day before the first hourly row posts). ``partition_date`` is the daily
    partition key (``YYYY-MM-DD``) and is used as the bronze ``dt`` partition.
    """
    if not rows:
        return None
    raw_bytes = ("\n".join(rows) + "\n").encode("utf-8")
    first_fields = rows[0].split()
    wbanno = first_fields[0] if first_fields else None
    meta = {
        "request_url": source_url,
        "request_params": {
            "station": station,
            "wbanno": wbanno,
            "year": year,
            "utc_date": partition_date.replace("-", ""),
            "row_count": len(rows),
        },
        "http_status": 200,
        "content_type": "text/plain",
        "charset": "utf-8",
        "content_encoding": "identity",
        "stored_encoding": "identity",
        "processor": PROCESSOR,
        "processor_version": processor_version,
    }
    return capture_bronze(
        SOURCE,
        COLLECTION,
        raw_bytes,
        meta,
        bronze_root=bronze_root,
        dt=partition_date,
        dedupe=True,
        ext="txt",
    )
