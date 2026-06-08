"""Write raw source bytes to the bronze layer.

Takes the exact bytes a processor received from a source, derives the standard
path/key, and writes the payload plus a ``.meta.json`` sidecar atomically
(temp file + rename).

Design constraints (from the bronze spec):

* **Bytes only.** Payloads are written byte-for-byte from ``response.content``;
  never from a decoded string. Decoding is left to a downstream (silver) layer.
* **Append-only / immutable.** Every written capture is a new, uniquely named
  file. Nothing is ever overwritten or deleted here.
* **Content-hash deduped.** Before writing, the payload's sha256 is compared to
  the newest capture for the same ``(source, collection, dt)`` partition; an
  identical payload is skipped. Re-capturing an overlapping window each tick
  therefore costs API calls but near-zero storage.
* **Non-fatal.** :func:`capture_bronze` never raises; any failure is logged as a
  warning and swallowed so the caller's normal processing continues.
* **Swappable root.** The bronze root is passed in by the caller (from its
  settings); nothing is hardcoded, keeping the S3 migration path open.
"""

import glob
import hashlib
import json
import os
import secrets
from datetime import UTC, datetime
from typing import Any

from grecohome_core.logging_config import get_logger

logger = get_logger(__name__)

# Bumped if the sidecar shape changes in a backward-incompatible way.
SCHEMA_VERSION = "v1"

# Map a media type + on-disk encoding to the file extension that reflects the
# *stored* form. Kept deliberately small; unknown types fall back to ``bin``.
_CONTENT_TYPE_EXT = {
    "application/json": "json",
    "text/json": "json",
    "text/csv": "csv",
    "application/csv": "csv",
    "application/xml": "xml",
    "text/xml": "xml",
    "text/plain": "txt",
}


def _ext_for(content_type: str | None, stored_encoding: str) -> str:
    """Pick the file extension for the *stored* bytes."""
    base = _CONTENT_TYPE_EXT.get((content_type or "").split(";")[0].strip().lower(), "bin")
    if stored_encoding and stored_encoding.lower() == "gzip":
        return f"{base}.gz"
    return base


def _utc_now_ms() -> int:
    """Current UTC time as Unix epoch milliseconds."""
    return int(datetime.now(UTC).timestamp() * 1000)


def _write_atomic(path: str, data: bytes) -> None:
    """Write ``data`` to ``path`` atomically via a temp file + rename.

    A half-written file never appears under the final name. ``os.replace`` is
    atomic on a local filesystem and maps cleanly to S3 put-once semantics later.
    The temp file lives in the same directory so the rename stays on one filesystem.
    """
    directory = os.path.dirname(path)
    os.makedirs(directory, exist_ok=True)
    tmp_path = os.path.join(directory, f".tmp_{secrets.token_hex(8)}")
    try:
        with open(tmp_path, "wb") as fh:
            fh.write(data)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp_path, path)
    except Exception:
        try:
            os.remove(tmp_path)
        except OSError:
            pass
        raise


def _meta_fetched_ms(meta_path: str) -> int:
    """Extract the fetched-at millis embedded in a sidecar filename.

    Filenames are ``{collection}_{fetched_ms}_{short_id}.meta.json``; parsing the
    name avoids opening every sidecar just to order them.
    """
    name = os.path.basename(meta_path)
    parts = name.split("_")
    try:
        return int(parts[-2])
    except (IndexError, ValueError):
        return 0


def _latest_sha256(partition_dir: str) -> str | None:
    """Return the sha256 of the newest capture in ``partition_dir``, or None.

    Best-effort: any error returns ``None`` so dedup degrades to "write".
    """
    try:
        metas = glob.glob(os.path.join(partition_dir, "*.meta.json"))
        if not metas:
            return None
        newest = max(metas, key=_meta_fetched_ms)
        with open(newest) as fh:
            return json.load(fh).get("sha256")
    except Exception:
        return None


def capture_bronze(
    source: str,
    collection: str,
    raw_bytes: bytes,
    meta: dict[str, Any],
    *,
    bronze_root: str,
    dt: str | None = None,
) -> str | None:
    """Write raw source bytes (and a sidecar) to the bronze layer.

    Best-effort and non-fatal: on any failure this logs a warning and returns
    ``None`` rather than raising, so the caller's normal processing continues.

    Args:
        source: Data provider, lowercase (e.g. ``"whoop"``).
        collection: Dataset within the source, lowercase (e.g. ``"recovery"``).
        raw_bytes: The exact response body as **bytes** (never a decoded string).
        meta: Provenance fields for the sidecar. ``sha256``, ``byte_size``,
            ``fetched_at``, ``fetched_at_unix_ms``, ``source``, ``collection``,
            and ``schema_version`` are filled in here; anything else (e.g.
            ``request_url``, ``http_status``, ``content_type``,
            ``processor_version``) is passed through. **Must not** contain secrets.
        bronze_root: Root directory for bronze output.
        dt: Partition date ``"YYYY-MM-DD"`` the payload belongs to. When omitted,
            the fetch-time UTC date is used. Pass the asset's partition date so
            hourly re-captures of a trailing day dedup against the right folder.

    Returns:
        The payload path written, ``None`` if the payload was deduped (identical
        to the latest capture for this partition) or if capture failed.
    """
    try:
        if not isinstance(raw_bytes, (bytes, bytearray)):
            # Guard the core invariant: bronze stores bytes, not decoded text.
            raise TypeError(f"raw_bytes must be bytes, got {type(raw_bytes).__name__}")

        fetched_ms = _utc_now_ms()
        fetched_dt = datetime.fromtimestamp(fetched_ms / 1000, tz=UTC)
        partition_dt = dt or fetched_dt.strftime("%Y-%m-%d")
        sha256 = hashlib.sha256(raw_bytes).hexdigest()

        partition_dir = os.path.join(bronze_root, source, collection, f"dt={partition_dt}")

        # Content-hash dedup: skip writing an identical payload.
        if _latest_sha256(partition_dir) == sha256:
            logger.debug(
                "bronze capture deduped",
                source=source,
                collection=collection,
                dt=partition_dt,
                sha256=sha256,
            )
            return None

        short_id = secrets.token_hex(3)  # 6 hex chars
        stored_encoding = meta.get("stored_encoding", "identity")
        ext = _ext_for(meta.get("content_type"), stored_encoding)

        rel_base = f"{collection}_{fetched_ms}_{short_id}"
        payload_path = os.path.join(partition_dir, f"{rel_base}.{ext}")
        sidecar_path = os.path.join(partition_dir, f"{rel_base}.meta.json")

        sidecar = {
            **meta,
            "source": source,
            "collection": collection,
            "fetched_at": fetched_dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
            "fetched_at_unix_ms": fetched_ms,
            "byte_size": len(raw_bytes),
            "sha256": sha256,
            "stored_encoding": stored_encoding,
            "schema_version": meta.get("schema_version", SCHEMA_VERSION),
        }
        sidecar_bytes = json.dumps(
            sidecar, separators=(",", ":"), sort_keys=True
        ).encode("utf-8")

        # Payload first, then sidecar -- if the sidecar write fails the payload
        # is still safely on disk.
        _write_atomic(payload_path, bytes(raw_bytes))
        _write_atomic(sidecar_path, sidecar_bytes)

        logger.debug(
            "bronze capture written",
            source=source,
            collection=collection,
            path=payload_path,
            byte_size=len(raw_bytes),
        )
        return payload_path
    except Exception as e:  # noqa: BLE001 - capture must never break processing
        logger.warning(
            "bronze capture failed",
            source=source,
            collection=collection,
            error=str(e),
        )
        return None
