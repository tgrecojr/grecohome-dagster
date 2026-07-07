"""The promote logic: relay staging files -> bronze (the only real logic here).

Reads ``RELAY_CAPTURE_DIR`` (read-only), tracks a per-stream **promoted-set** in
``LOCATION_STATE_DIR``, and lands each new staging file in bronze via
:func:`grecohome_location.capture.capture_location`.

Idempotency is layered so promotion is **exactly-once in bronze** across crashes:

1. *Primary guard* — the promoted-set, keyed by the unique staging **filename**
   (never content, so byte-identical distinct POSTs both land).
2. *Durable backstop / rebuild key* — a staging file is "already promoted" iff a
   bronze sidecar in its ``dt`` partition carries a matching ``staging_file``. This
   covers the crash window (bronze written, promoted-set not yet advanced) and lets a
   lost promoted-set rebuild itself from bronze. It does not depend on the core
   writer's ambiguous ``None`` return.

Each run promotes ``(staging window) - (promoted-set ∪ bronze sidecars)`` and then
persists the promoted-set **pruned to the window** (bounded; the sidecars are the
durable record). This module never writes, moves, or deletes under the relay dir.
"""

from __future__ import annotations

import json
import os
import re
import secrets
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from grecohome_core.checks.bronze_reads import (
    collection_dir,
    iter_payloads,
    read_sidecar,
)
from grecohome_core.logging_config import get_logger
from grecohome_location.capture import SOURCE, capture_location

logger = get_logger(__name__)

#: Only these staging basenames are real captures: ``{received_unix_ms}_{6 hex}.json``.
#: Anything else (a transient ``.tmp*``, a stray file) is ignored.
STAGING_RE = re.compile(r"^(\d+)_[0-9a-f]{6}\.json$")


@dataclass(frozen=True)
class StagingFile:
    """One promotable staging file, with everything the writer needs."""

    basename: str
    path: str
    dt: str  # UTC receipt date "YYYY-MM-DD" from the staging path
    received_ms: int  # true receipt time from the filename


@dataclass
class PromoteReport:
    """What one stream's promote run did (surfaced as Dagster run metadata)."""

    stream: str
    scanned: int = 0
    promoted: int = 0
    already: int = 0
    bytes_promoted: int = 0
    failed: int = 0
    oldest_received_ms: int | None = None
    newest_received_ms: int | None = None


# ---------------------------------------------------------------------------
# Staging discovery (read-only)
# ---------------------------------------------------------------------------
def window_dates(now: datetime, window_days: int) -> list[str]:
    """The trailing ``window_days`` UTC dates (incl. today), newest last."""
    n = max(1, window_days)
    days = [(now - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(n)]
    return sorted(set(days))


def parse_received_ms(basename: str) -> int | None:
    """Epoch-ms receipt time from a staging basename, or None if it doesn't match."""
    m = STAGING_RE.match(basename)
    return int(m.group(1)) if m else None


def list_staging_files(capture_dir: str, stream: str, dates: list[str]) -> list[StagingFile]:
    """Every matching staging file for ``stream`` across ``dates`` (missing dirs skipped)."""
    out: list[StagingFile] = []
    for dt in dates:
        pdir = os.path.join(capture_dir, stream, f"dt={dt}")
        try:
            names = os.listdir(pdir)
        except OSError:
            continue  # partition not present (yet) or pruned — not an error
        for name in names:
            received_ms = parse_received_ms(name)
            if received_ms is None:
                continue  # .tmp* / junk
            path = os.path.join(pdir, name)
            if os.path.isfile(path):
                out.append(StagingFile(basename=name, path=path, dt=dt, received_ms=received_ms))
    out.sort(key=lambda f: (f.received_ms, f.basename))
    return out


# ---------------------------------------------------------------------------
# Promoted-set state (per stream, outside BRONZE_ROOT)
# ---------------------------------------------------------------------------
def _state_path(state_dir: str, stream: str) -> str:
    return os.path.join(state_dir, f"promoted_{stream}.json")


def load_promoted_set(state_dir: str, stream: str) -> set[str]:
    """Load the persisted promoted-set for ``stream`` (empty if missing/unreadable)."""
    try:
        with open(_state_path(state_dir, stream)) as fh:
            data = json.load(fh)
    except (OSError, json.JSONDecodeError):
        return set()
    promoted = data.get("promoted") if isinstance(data, dict) else None
    return set(promoted) if isinstance(promoted, list) else set()


def save_promoted_set(state_dir: str, stream: str, basenames: set[str]) -> None:
    """Atomically persist ``stream``'s promoted-set (temp file + rename)."""
    os.makedirs(state_dir, exist_ok=True)
    path = _state_path(state_dir, stream)
    payload = json.dumps(
        {"stream": stream, "promoted": sorted(basenames)}, separators=(",", ":")
    ).encode("utf-8")
    tmp = path + f".tmp_{secrets.token_hex(6)}"
    try:
        with open(tmp, "wb") as fh:
            fh.write(payload)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, path)
    except Exception:
        try:
            os.remove(tmp)
        except OSError:
            pass
        raise


def promoted_from_bronze(bronze_root: str, stream: str, dates: list[str]) -> set[str]:
    """The ``staging_file`` values already recorded in bronze sidecars for ``dates``.

    The durable backstop / rebuild key: reading these makes promotion exactly-once in
    bronze even if the promoted-set is lost or a crash happened mid-run.
    """
    coll_dir = collection_dir(bronze_root, SOURCE, stream)
    out: set[str] = set()
    for dt in dates:
        pdir = os.path.join(coll_dir, f"dt={dt}")
        if not os.path.isdir(pdir):
            continue
        for payload in iter_payloads(pdir):
            sidecar = read_sidecar(payload)
            staging_file = (sidecar or {}).get("staging_file")
            if isinstance(staging_file, str):
                out.add(staging_file)
    return out


# ---------------------------------------------------------------------------
# The promote run
# ---------------------------------------------------------------------------
def _resolve_todo(
    staging: list[StagingFile],
    promoted: set[str],
    *,
    bronze_root: str,
    stream: str,
    dates: list[str],
) -> tuple[list[StagingFile], set[str]]:
    """Split ``staging`` into work-to-do vs the effective already-promoted set.

    The bronze sidecar backstop is only consulted when a staging file is *not* in the
    promoted-set — so the steady state (everything already promoted) never re-scans
    days of sidecars; the scan only runs when there's genuinely new work or the
    promoted-set was lost.
    """
    candidates = [f for f in staging if f.basename not in promoted]
    bronze_promoted = (
        promoted_from_bronze(bronze_root, stream, dates) if candidates else set()
    )
    todo = [f for f in candidates if f.basename not in bronze_promoted]
    return todo, promoted | bronze_promoted


def unpromoted_staging(
    *,
    capture_dir: str,
    bronze_root: str,
    state_dir: str,
    stream: str,
    now: datetime,
    window_days: int,
) -> list[StagingFile]:
    """Staging files still owed to bronze (read-only; used by the promote-lag check).

    Agrees with :func:`promote_stream` on "what is still owed": promoted-set first,
    bronze sidecars as the backstop for anything not in it.
    """
    dates = window_dates(now, window_days)
    staging = list_staging_files(capture_dir, stream, dates)
    todo, _ = _resolve_todo(
        staging, load_promoted_set(state_dir, stream),
        bronze_root=bronze_root, stream=stream, dates=dates,
    )
    return todo


def promote_stream(
    *,
    capture_dir: str,
    bronze_root: str,
    state_dir: str,
    stream: str,
    now: datetime | None = None,
    window_days: int,
) -> PromoteReport:
    """Promote every new staging file for one stream and advance the promoted-set."""
    now = now or datetime.now(UTC)
    dates = window_dates(now, window_days)
    staging = list_staging_files(capture_dir, stream, dates)
    staging_basenames = {f.basename for f in staging}
    todo, effective = _resolve_todo(
        staging, load_promoted_set(state_dir, stream),
        bronze_root=bronze_root, stream=stream, dates=dates,
    )

    report = PromoteReport(stream=stream, scanned=len(staging))
    report.already = len(staging) - len(todo)
    receipts: list[int] = []
    for f in todo:
        try:
            with open(f.path, "rb") as fh:
                raw = fh.read()
        except OSError as e:
            report.failed += 1
            logger.warning("staging read failed", stream=stream, file=f.basename, error=str(e))
            continue
        path = capture_location(
            raw,
            collection=stream,
            dt=f.dt,
            received_ms=f.received_ms,
            staging_file=f.basename,
            bronze_root=bronze_root,
        )
        if path is None:
            # Core writer swallowed an error (rare with dedupe=False/200). Leave it
            # un-promoted so a later run retries; bronze holds nothing for it.
            report.failed += 1
            logger.warning("bronze capture returned None", stream=stream, file=f.basename)
            continue
        effective.add(f.basename)
        report.promoted += 1
        report.bytes_promoted += len(raw)
        receipts.append(f.received_ms)

    if receipts:
        report.oldest_received_ms = min(receipts)
        report.newest_received_ms = max(receipts)

    # Persist the promoted-set pruned to the current window: anything outside it is
    # gone from staging and durably backstopped by bronze sidecars, so it need not be
    # carried forward (keeps the state file bounded).
    save_promoted_set(state_dir, stream, {bn for bn in effective if bn in staging_basenames})
    return report
