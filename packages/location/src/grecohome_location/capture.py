"""Location capture adapter over the shared core bronze writer.

Promotes one relay staging file (a byte-exact Overland/OwnTracks POST body) into
bronze. Two collections: ``location/overland`` and ``location/owntracks``.

Key distinctions the sidecar preserves:

* ``fetched_at`` (writer-stamped) = when bronze *adopted* the file (promote time).
* ``received_at`` / ``received_unix_ms`` = the TRUE receipt time, parsed from the
  staging filename (never present inside the payload).
* ``staging_file`` = the unique staging basename; the promoter's idempotency key and
  audit link back to the relay's capture.

``dedupe=False``: idempotency is keyed on the staging *filename*, never on content —
two distinct byte-identical POSTs (e.g. a re-sent OwnTracks ping) must both land.
The payload is stored ``raw`` / byte-exact (``sha256(bronze) == sha256(staging)``).
No secret ever reaches bronze (the relay keeps the auth token header/query-only).
"""

from __future__ import annotations

from datetime import UTC, datetime

from grecohome_core.bronze import capture_bronze
from grecohome_location import __version__

SOURCE = "location"
PROCESSOR = "grecohome-location"

#: The two fixed collections, one per relay staging subdir / ingest route.
STREAMS = ("overland", "owntracks")


def iso_from_ms(received_ms: int) -> str:
    """UTC ISO-8601 (millisecond precision, ``Z`` suffix) for an epoch-ms instant."""
    dt = datetime.fromtimestamp(received_ms / 1000, tz=UTC)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def capture_location(
    raw_bytes: bytes,
    *,
    collection: str,
    dt: str,
    received_ms: int,
    staging_file: str,
    bronze_root: str,
    processor_version: str = __version__,
) -> str | None:
    """Capture one relay staging file (raw bytes) into bronze.

    Args:
        raw_bytes: The staging file's exact bytes (the verbatim POST body).
        collection: ``"overland"`` or ``"owntracks"`` (the staging subdir).
        dt: UTC receipt date ``"YYYY-MM-DD"`` parsed from the staging path (bronze ``dt``).
        received_ms: True receipt time (epoch ms), parsed from the staging filename.
        staging_file: The staging basename (idempotency key + audit link).
        bronze_root: Root directory for bronze output.

    Returns:
        The bronze payload path, or ``None`` if the underlying core writer swallowed
        a capture error. Identical content is **not** deduped (see module docstring).
    """
    meta = {
        "http_status": 200,  # only accepted 2xx POSTs are staged by the relay
        "content_type": "application/json",
        "charset": "utf-8",
        "content_encoding": "identity",
        "stored_encoding": "identity",
        "processor": PROCESSOR,
        "processor_version": processor_version,
        "capture_mode": "raw",  # byte-exact request body (not reserialized)
        "ingest_route": f"/{collection}",
        "received_at": iso_from_ms(received_ms),  # TRUE receipt time (from filename)
        "received_unix_ms": received_ms,
        "staging_file": staging_file,  # idempotency key + audit link to source
        "redacted_fields": [],  # empty by construction (no secret in the body)
    }
    # dt = receipt date (Lingo-style); dedupe=False so byte-identical distinct POSTs
    # both land; ext="json" explicitly (bodies are JSON).
    return capture_bronze(
        SOURCE,
        collection,
        raw_bytes,
        meta,
        bronze_root=bronze_root,
        dt=dt,
        dedupe=False,
        ext="json",
    )
