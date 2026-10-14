"""Write raw source bytes to the bronze layer.

Takes the exact bytes a processor received from a source, derives the standard
path/key, and writes the payload plus a ``.meta.json`` sidecar atomically
(temp file + rename).

Design constraints (from the bronze spec):

* **Bytes only.** Payloads are written byte-for-byte from ``response.content``;
  never from a decoded string. Decoding is left to a downstream (silver) layer.
* **Append-only / immutable.** Every written capture is a new, uniquely named
  file. Nothing is ever overwritten or deleted here.
* **Content-hash dedup (opt-in).** With ``dedupe=True`` (the default), the
  payload's sha256 is compared to the newest capture for the same
  ``(source, collection, dt)`` partition and an identical payload is skipped —
  so a source that re-captures an overlapping window each tick (e.g. Whoop, which
  rescores) costs API calls but near-zero storage. Callers pass ``dedupe=False``
  where every capture must be kept (e.g. Garmin's fan-out paths, where several
  distinct requests write one partition folder).
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
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, Literal

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


# @spec CORE-BRONZE-003
class CaptureOutcome(StrEnum):
    """What a capture did."""

    WRITTEN = "written"
    SKIPPED_IDENTICAL = "skipped_identical"
    SKIPPED_NON_2XX = "skipped_non_2xx"
    FAILED = "failed"


@dataclass(frozen=True)
class CaptureResult:
    """The outcome of one ``capture_bronze`` call; ``path`` is set only when written."""

    outcome: CaptureOutcome
    path: str | None = None


# Attempts at a fresh ``short_id`` before an exclusive-create collision is a failure.
_NAME_ATTEMPTS = 5


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

    A half-written file never appears under the final name, and an existing file is
    never replaced: the final name is created with ``os.link`` (atomic, and failing with
    ``FileExistsError`` if the name exists), which maps cleanly to S3 put-if-absent later.
    The temp file lives in the same directory so the link stays on one filesystem.
    """
    directory = os.path.dirname(path)
    os.makedirs(directory, exist_ok=True)
    tmp_path = os.path.join(directory, f".tmp_{secrets.token_hex(8)}")
    try:
        with open(tmp_path, "wb") as fh:
            fh.write(data)
            fh.flush()
            os.fsync(fh.fileno())
        # Exclusive create: link() refuses an existing name (FileExistsError), so a
        # capture can never replace another one.
        os.link(tmp_path, path)
    finally:
        _remove_quietly(tmp_path)


def _remove_quietly(path: str) -> None:
    try:
        os.remove(path)
    except OSError:
        pass


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


def _canonical(value: Any) -> str:
    """A key-order-independent JSON form, for comparing ``request_params``."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _matching_sha256(
    partition_dir: str, scope: str, request_params: Any
) -> str | None:
    """The sha256 of the earlier capture skip-identical compares with, or None.

    ``folder``: the newest sidecar in the folder. ``request``: the newest sidecar whose
    ``request_params`` equal ``request_params``. Best-effort: any error, or no match,
    returns ``None`` so skip-identical degrades to "write".
    """
    try:
        metas = sorted(
            glob.glob(os.path.join(partition_dir, "*.meta.json")),
            key=_meta_fetched_ms,
            reverse=True,
        )
        if scope == "folder":
            metas = metas[:1]
        wanted = _canonical(request_params)
        for meta_path in metas:
            with open(meta_path) as fh:
                sidecar = json.load(fh)
            if scope == "folder" or _canonical(sidecar.get("request_params")) == wanted:
                return sidecar.get("sha256")
        return None
    except Exception:
        return None


# @spec CORE-BRONZE-001, CORE-BRONZE-002, CORE-BRONZE-004, CORE-BRONZE-005, CORE-BRONZE-006
# @spec CORE-BRONZE-007, CORE-BRONZE-008, CORE-BRONZE-009, CORE-BRONZE-010, CORE-BRONZE-011
# @spec CORE-BRONZE-012, CORE-BRONZE-013, CORE-BRONZE-014, CORE-BRONZE-015
def capture_bronze(
    source: str,
    collection: str,
    raw_bytes: bytes,
    meta: dict[str, Any],
    *,
    bronze_root: str,
    dt: str | None = None,
    dedupe: bool = True,
    dedupe_scope: Literal["folder", "request"] = "folder",
    ext: str | None = None,
) -> CaptureResult:
    """Write raw source bytes (and a sidecar) to the bronze layer.

    Best-effort and non-fatal: on any failure this logs a warning and reports a
    ``failed`` outcome rather than raising, so the caller's normal processing continues.

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
        dedupe: When True (default), skip writing a payload identical to the earlier
            capture ``dedupe_scope`` selects in this ``(source, collection, dt)``
            folder. Pass False to keep every capture.
        dedupe_scope: ``"folder"`` (default) compares with the folder's newest
            capture (one request per folder); ``"request"`` compares with the newest
            capture whose ``request_params`` equal this one's (several requests per
            folder).
        ext: Explicit file extension for the stored payload (e.g. ``"json"``,
            ``"zip"``). When None, it is derived from ``content_type``.

    Returns:
        A :class:`CaptureResult`: ``written`` (with the payload path),
        ``skipped_identical``, ``skipped_non_2xx`` or ``failed``.
    """
    try:
        if not isinstance(raw_bytes, (bytes, bytearray)):
            # Guard the core invariant: bronze stores bytes, not decoded text.
            raise TypeError(f"raw_bytes must be bytes, got {type(raw_bytes).__name__}")

        # Defense in depth: bronze stores source DATA, never an HTTP error envelope.
        # When the caller records ``http_status``, only 2xx responses are persisted,
        # so an upstream auth/error body (e.g. a 401 "Authorization was not valid")
        # can't land as a payload and poison a content/integrity check. Callers that
        # don't track status (they only ever pass 200) are unaffected.
        http_status = meta.get("http_status")
        if isinstance(http_status, int) and not (200 <= http_status < 300):
            logger.warning(
                "bronze capture skipped: non-2xx response",
                source=source,
                collection=collection,
                http_status=http_status,
            )
            return CaptureResult(CaptureOutcome.SKIPPED_NON_2XX)

        fetched_ms = _utc_now_ms()
        fetched_dt = datetime.fromtimestamp(fetched_ms / 1000, tz=UTC)
        partition_dt = dt or fetched_dt.strftime("%Y-%m-%d")
        sha256 = hashlib.sha256(raw_bytes).hexdigest()

        partition_dir = os.path.join(bronze_root, source, collection, f"dt={partition_dt}")

        # Content-hash dedup (opt-in): skip writing an identical payload.
        if dedupe and _matching_sha256(
            partition_dir, dedupe_scope, meta.get("request_params")
        ) == sha256:
            logger.debug(
                "bronze capture deduped",
                source=source,
                collection=collection,
                dt=partition_dt,
                sha256=sha256,
            )
            return CaptureResult(CaptureOutcome.SKIPPED_IDENTICAL)

        stored_encoding = meta.get("stored_encoding", "identity")
        file_ext = ext if ext is not None else _ext_for(meta.get("content_type"), stored_encoding)

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

        payload_path = _write_pair(
            partition_dir, collection, fetched_ms, file_ext, bytes(raw_bytes), sidecar_bytes
        )
        logger.debug(
            "bronze capture written",
            source=source,
            collection=collection,
            path=payload_path,
            byte_size=len(raw_bytes),
        )
        return CaptureResult(CaptureOutcome.WRITTEN, payload_path)
    except Exception as e:  # noqa: BLE001 - capture must never break processing
        logger.warning(
            "bronze capture failed",
            source=source,
            collection=collection,
            error=str(e),
        )
        return CaptureResult(CaptureOutcome.FAILED)


def _write_pair(
    partition_dir: str,
    collection: str,
    fetched_ms: int,
    file_ext: str,
    payload: bytes,
    sidecar_bytes: bytes,
) -> str:
    """Write a payload then its sidecar under a fresh, never-existing name; return the path.

    A name collision draws a new ``short_id`` (up to ``_NAME_ATTEMPTS``). If the sidecar
    write fails, the payload just written is deleted, so a payload never lacks its
    sidecar, and the error propagates.
    """
    for _ in range(_NAME_ATTEMPTS):
        rel_base = f"{collection}_{fetched_ms}_{secrets.token_hex(3)}"  # 6 hex chars
        payload_path = os.path.join(partition_dir, f"{rel_base}.{file_ext}")
        sidecar_path = os.path.join(partition_dir, f"{rel_base}.meta.json")
        try:
            _write_atomic(payload_path, payload)
        except FileExistsError:
            continue
        try:
            _write_atomic(sidecar_path, sidecar_bytes)
        except FileExistsError:
            _remove_quietly(payload_path)
            continue
        except BaseException:
            _remove_quietly(payload_path)
            raise
        return payload_path
    raise FileExistsError(f"no free capture name after {_NAME_ATTEMPTS} attempts")
