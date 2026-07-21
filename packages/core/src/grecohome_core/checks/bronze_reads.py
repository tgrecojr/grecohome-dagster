"""Pure-Python, read-only helpers for inspecting the bronze tree.

Everything a check needs to *read* bronze lives here: partition discovery,
sidecar-safe payload listing, freshness from sidecars, distinct event dates,
schema signatures, payload classification and byte-integrity. No DuckDB — the
stdlib reads JSON/CSV/txt directly, which sidesteps the schema-union sidecar
gotcha entirely (we never glob a column union; signatures read one exact file).

**Two invariants enforced here so no subject re-hits them:**

1. *Sidecars are never read as payloads.* ``*.meta.json`` ends in ``.json`` and
   sits beside every payload. :func:`iter_payloads` excludes them.
2. *Schema signatures read one exact payload file*, never a glob — so a sidecar's
   fields (``sha256``, ``fetched_at``, ...) can never contaminate an inferred
   column/key union.

Nothing in this module ever writes, moves, or deletes under the bronze root.
"""

from __future__ import annotations

import csv
import glob
import hashlib
import json
import os
from datetime import UTC, date, datetime

META_SUFFIX = ".meta.json"

# --- content classification (ported verbatim from sweep_streams.py) ------------

#: JSON keys whose value is the real data array; an *empty* one of these (with no
#: other substantive keys) is an "empty wrapper", not data.
DATA_ARRAY_KEYS = ("records", "data", "items", "results", "activities")

#: Keys that, present without any populated data array, mark an error envelope.
#: ``status`` alone is too ambiguous to count (see :func:`classify_json`).
ERROR_KEYS = ("error", "errors", "message", "fault", "exception", "status_code")

#: Pagination keys that don't count as "substantive" content beside an empty
#: data array.
_PAGINATION_KEYS = ("next_token", "nextToken", "next", "paging")


# ---------------------------------------------------------------------------
# Path discovery
# ---------------------------------------------------------------------------
def collection_dir(bronze_root: str, source: str, collection: str) -> str:
    """The directory holding a collection's ``dt=`` partitions."""
    return os.path.join(bronze_root, source, collection)


def list_partition_dirs(coll_dir: str) -> list[tuple[str, str]]:
    """Return ``(dt_string, path)`` for every ``dt=YYYY-MM-DD`` folder, sorted.

    Missing collection directories yield an empty list (a not-yet-captured
    collection is a valid state the caller decides how to treat).
    """
    if not os.path.isdir(coll_dir):
        return []
    out: list[tuple[str, str]] = []
    for name in os.listdir(coll_dir):
        if name.startswith("dt="):
            path = os.path.join(coll_dir, name)
            if os.path.isdir(path):
                out.append((name[3:], path))
    out.sort(key=lambda t: t[0])
    return out


def trailing_partition_dirs(coll_dir: str, recent_partitions: int | None) -> list[tuple[str, str]]:
    """The trailing ``recent_partitions`` partition folders (all of them if None)."""
    parts = list_partition_dirs(coll_dir)
    if recent_partitions is None or recent_partitions <= 0:
        return parts
    return parts[-recent_partitions:]


def iter_payloads(partition_dir: str) -> list[str]:
    """Every payload file in one partition folder, **excluding** sidecars."""
    return sorted(
        f
        for f in glob.glob(os.path.join(partition_dir, "*"))
        if os.path.isfile(f) and not f.endswith(META_SUFFIX)
    )


def sidecar_for(payload_path: str) -> str | None:
    """Resolve a payload's ``.meta.json`` sidecar, or None.

    Handles both layouts: ``<stem>.meta.json`` (what :mod:`grecohome_core.bronze`
    writes — the extension is replaced) and ``<payload>.meta.json`` (the suffix is
    appended), so the helper is robust to either capture convention.
    """
    stem, _ = os.path.splitext(payload_path)
    for cand in (stem + META_SUFFIX, payload_path + META_SUFFIX):
        if os.path.exists(cand):
            return cand
    return None


def read_sidecar(payload_path: str) -> dict | None:
    """Load a payload's sidecar as a dict, or None if missing/unparseable."""
    meta_path = sidecar_for(payload_path)
    if meta_path is None:
        return None
    try:
        with open(meta_path) as fh:
            return json.load(fh)
    except (OSError, json.JSONDecodeError):
        return None


def _fetched_ms_from_name(meta_path: str) -> int:
    """Pull the fetched-at millis out of a sidecar filename without opening it.

    Names are ``{collection}_{fetched_ms}_{short_id}.meta.json``; collections may
    contain underscores (``body_measurement``) so we index from the right.
    """
    name = os.path.basename(meta_path)[: -len(META_SUFFIX)]
    parts = name.split("_")
    try:
        return int(parts[-2])
    except (IndexError, ValueError):
        return 0


# ---------------------------------------------------------------------------
# Freshness
# ---------------------------------------------------------------------------
def newest_fetch(coll_dir: str, recent_partitions: int | None) -> tuple[datetime | None, int]:
    """Return ``(newest_fetched_at, sidecar_count)`` over the trailing partitions.

    The newest fetch time is read from sidecar filenames (cheap; no file opens) and
    returned as a UTC ``datetime``. ``sidecar_count`` is how many sidecars were seen
    across the inspected partitions — zero means the collection has no captures in
    the window (which the freshness check treats as a problem unless the collection
    is marked ``expected_empty``).
    """
    newest_ms = 0
    count = 0
    for _dt, pdir in trailing_partition_dirs(coll_dir, recent_partitions):
        for meta in glob.glob(os.path.join(pdir, "*" + META_SUFFIX)):
            count += 1
            ms = _fetched_ms_from_name(meta)
            if ms > newest_ms:
                newest_ms = ms
    if count == 0 or newest_ms == 0:
        return None, count
    return datetime.fromtimestamp(newest_ms / 1000, tz=UTC), count


# ---------------------------------------------------------------------------
# Event dates / completeness
# ---------------------------------------------------------------------------
def parse_event_date(value: object) -> date | None:
    """Best-effort parse of a timestamp-ish value to a calendar ``date``.

    Handles ISO dates/datetimes (``2024-12-31`` / ``2024-12-31T08:00:00Z`` /
    ``2024-12-31 08:00:00``) and the common US ``MM/DD/YYYY`` / ``MM-DD-YYYY``
    forms. Returns None when nothing parses (the row is then ignored rather than
    crashing the check).
    """
    if value is None:
        return None
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(float(value), tz=UTC).date()
        except (OverflowError, OSError, ValueError):
            return None
    s = str(value).strip()
    if not s:
        return None
    # ISO date or datetime: the leading 10 chars are YYYY-MM-DD.
    try:
        return date.fromisoformat(s[:10])
    except ValueError:
        pass
    for fmt in ("%m/%d/%Y", "%m-%d-%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(s.split(" ")[0].split("T")[0], fmt).date()
        except ValueError:
            continue
    return None


def _dig(record: object, dotted: str) -> object:
    """Follow a dotted path into nested dicts; None if any hop is missing."""
    cur = record
    for key in dotted.split("."):
        if not isinstance(cur, dict) or key not in cur:
            return None
        cur = cur[key]
    return cur


def _payload_event_dates(payload_path: str, *, unnest_records: bool, field: str) -> set[date]:
    """Distinct event dates inside one JSON payload via a record field."""
    try:
        with open(payload_path, "rb") as fh:
            obj = json.loads(fh.read() or b"null")
    except (OSError, json.JSONDecodeError):
        return set()
    if obj is None:
        return set()
    if unnest_records and isinstance(obj, dict):
        records = obj.get("records") or []
    elif isinstance(obj, list):
        records = obj
    else:
        records = [obj]
    out: set[date] = set()
    for rec in records:
        d = parse_event_date(_dig(rec, field))
        if d is not None:
            out.add(d)
    return out


def _csv_event_dates(payload_path: str, column: str) -> set[date]:
    """Distinct event dates inside one CSV payload via a header column."""
    out: set[date] = set()
    try:
        with open(payload_path, newline="", encoding="utf-8", errors="replace") as fh:
            reader = csv.DictReader(fh)
            if reader.fieldnames is None or column not in reader.fieldnames:
                return out
            for row in reader:
                d = parse_event_date(row.get(column))
                if d is not None:
                    out.add(d)
    except OSError:
        return out
    return out


def distinct_event_dates(
    coll_dir: str,
    *,
    event_date_source: str,
    event_date_field: str | None,
    reader: str,
    unnest_records: bool,
    recent_partitions: int | None,
) -> list[date]:
    """Sorted distinct event dates for a collection over the trailing partitions.

    For ``event_date_source == "partition"`` the event date *is* the ``dt=`` folder
    (capture-once/immutable sources). For ``"payload"`` the date is read from inside
    each payload (a JSON record field or a CSV column) — this is what makes Lingo
    glucose and Whoop snapshots correct, where ``dt`` is the *fetch* date, not the
    event date.
    """
    parts = trailing_partition_dirs(coll_dir, recent_partitions)
    if event_date_source == "partition":
        out: set[date] = set()
        for dt_str, _pdir in parts:
            d = parse_event_date(dt_str)
            if d is not None:
                out.add(d)
        return sorted(out)

    if event_date_source == "payload" and event_date_field:
        out = set()
        for _dt, pdir in parts:
            for payload in iter_payloads(pdir):
                if reader == "csv":
                    out |= _csv_event_dates(payload, event_date_field)
                else:
                    out |= _payload_event_dates(
                        payload, unnest_records=unnest_records, field=event_date_field
                    )
        return sorted(out)

    return []


def find_gaps(dates: list[date], cadence_days: int) -> list[tuple[date, date, int]]:
    """Consecutive-date gaps larger than ``cadence_days``.

    Returns ``(after, before, missing_days)`` for each gap, where ``missing_days``
    is the count of absent calendar days between two present event dates. A gap of
    exactly ``cadence_days`` is allowed; only strictly larger gaps are surfaced.
    """
    gaps: list[tuple[date, date, int]] = []
    for a, b in zip(dates, dates[1:], strict=False):
        delta = (b - a).days
        if delta > cadence_days:
            gaps.append((a, b, delta - 1))
    return gaps


# ---------------------------------------------------------------------------
# Schema signature
# ---------------------------------------------------------------------------
def _payload_signature(payload: str, *, reader: str, unnest_records: bool) -> list[str] | None:
    """The top-level signature of **one** exact payload file (None if unreadable).

    * JSON ``{"records": [...]}`` (``unnest_records``): sorted keys of one record.
    * Flat JSON object: sorted top-level keys (hive ``dt`` is not in the payload).
    * CSV: sorted column names.
    * txt: the field count of the first non-empty row, as a ``"fields=<n>"`` token.

    Reading one exact file (never a glob) is what keeps sidecar fields out.
    """
    try:
        if reader == "json":
            with open(payload, "rb") as fh:
                obj = json.loads(fh.read() or b"null")
            if unnest_records and isinstance(obj, dict):
                records = obj.get("records") or []
                target = records[0] if records else {}
            elif isinstance(obj, list):
                target = obj[0] if obj else {}
            else:
                target = obj if isinstance(obj, dict) else {}
            return sorted(target.keys()) if isinstance(target, dict) else []
        if reader == "csv":
            with open(payload, newline="", encoding="utf-8", errors="replace") as fh:
                header = next(csv.reader(fh), [])
            return sorted(c for c in header if c != "dt")
        # txt: signature is the field count of the first non-empty row.
        with open(payload, encoding="utf-8", errors="replace") as fh:
            for line in fh:
                if line.strip():
                    return [f"fields={len(line.split())}"]
        return ["fields=0"]
    except (OSError, json.JSONDecodeError):
        return None


def schema_signature(
    coll_dir: str,
    *,
    reader: str,
    unnest_records: bool,
    recent_partitions: int | None,
) -> list[str] | None:
    """A stable top-level signature of the payload shape, from the **richest**
    representative payload across the trailing partitions.

    Picking the *single newest* file would let a data-sparse "stub" define the
    schema: sources like Garmin emit a thin payload for the most recent day before
    it finishes syncing — whole optional sections are absent, and a thin-day-only
    field occasionally appears — which trips drift every run until the day fills in.
    Optional sections only exist when data does, so the fullest recent payload (the
    most top-level tokens; ties resolved to the newest) is the best sample of the
    source's true contract. A real schema change still moves this signature: an
    added field makes the richest payload richer; a field dropped across *all*
    recent days lowers every candidate.

    **Empty payloads are ignored entirely.** An empty ``[]`` / ``{}`` payload has a
    zero-key signature — it has no schema to speak of, so it can neither define nor
    contradict a baseline. Sparse collections (e.g. Garmin ``max_metrics``: VO2max
    only repopulates after a qualifying activity, so most days write ``[]``) would
    otherwise record an empty baseline and then "drift" the first time real data
    lands. Skipping empties keys the signature off actual payloads whenever they
    appear and stays silent on empty days.

    Returns None when there is no *non-empty* payload to read in the window.
    """
    best: list[str] | None = None
    # Newest partition first, so an equal-richness tie keeps the newer payload.
    for _dt, pdir in reversed(trailing_partition_dirs(coll_dir, recent_partitions)):
        payloads = iter_payloads(pdir)
        if not payloads:
            continue
        sig = _payload_signature(payloads[-1], reader=reader, unnest_records=unnest_records)
        if not sig:  # None (unreadable) or [] (empty payload: no schema to compare)
            continue
        if best is None or len(sig) > len(best):
            best = sig
    return best


# ---------------------------------------------------------------------------
# Content classification (ported from sweep_streams.py)
# ---------------------------------------------------------------------------
def classify_json(obj: object) -> str:
    """Classify a parsed JSON payload as DATA / EMPTY_* / ERROR_LIKE.

    Mirrors ``sweep_streams.classify_json`` exactly: an empty data-array wrapper is
    only "empty" when nothing substantive sits beside it (pagination keys don't
    count); an error envelope requires a *real* error key (``status`` alone is too
    ambiguous to count).
    """
    if isinstance(obj, list):
        return "EMPTY_LIST" if len(obj) == 0 else "DATA"
    if isinstance(obj, dict):
        if len(obj) == 0:
            return "EMPTY_OBJECT"
        for k in DATA_ARRAY_KEYS:
            if k in obj and isinstance(obj[k], list) and len(obj[k]) == 0:
                others = [kk for kk in obj if kk not in (k, *_PAGINATION_KEYS)]
                if not others:
                    return "EMPTY_WRAPPER"
        lower = {k.lower() for k in obj}
        has_error = any(e in lower for e in ERROR_KEYS)
        has_data_array = any(
            isinstance(obj.get(k), list) and len(obj[k]) > 0 for k in DATA_ARRAY_KEYS
        )
        if has_error and not has_data_array:
            if any(e in lower for e in ("error", "errors", "fault", "exception")):
                return "ERROR_LIKE"
        return "DATA"
    return "DATA"  # scalar — unusual but not empty


def classify_payload(payload_path: str, sidecar: dict | None) -> str:
    """Classify one capture, sidecar status first.

    Order mirrors ``sweep_streams``: a non-2xx ``http_status`` short-circuits to
    HTTP_ERROR; non-JSON payloads are CSV_DATA / TXT_DATA (or EMPTY_FILE); JSON is
    parsed and run through :func:`classify_json`.
    """
    status = (sidecar or {}).get("http_status")
    if status is not None:
        try:
            if not (200 <= int(status) < 300):
                return "HTTP_ERROR"
        except (TypeError, ValueError):
            pass
    if not payload_path.endswith(".json"):
        size = os.path.getsize(payload_path)
        if size == 0:
            return "EMPTY_FILE"
        return "TXT_DATA" if payload_path.endswith(".txt") else "CSV_DATA"
    try:
        with open(payload_path, "rb") as fh:
            obj = json.loads(fh.read() or b"null")
    except (OSError, json.JSONDecodeError):
        return "UNPARSEABLE"
    if obj is None:
        return "EMPTY_FILE"
    return classify_json(obj)


#: Classes that represent real, usable data.
DATA_CLASSES = frozenset({"DATA", "CSV_DATA", "TXT_DATA"})
#: Classes that represent a legitimately-empty payload (vs. an outright error).
EMPTY_CLASSES = frozenset({"EMPTY_LIST", "EMPTY_OBJECT", "EMPTY_WRAPPER", "EMPTY_FILE"})


# ---------------------------------------------------------------------------
# Byte integrity (ported from verify_bronze.py)
# ---------------------------------------------------------------------------
def _sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def verify_integrity(payload_path: str, sidecar: dict | None) -> list[str]:
    """Byte-level integrity/validity issues for one capture (empty == clean).

    Returns a list of human-readable problem strings (empty when sound):

    * INTEGRITY — on-disk sha256 disagrees with the sidecar's ``sha256``.
    * CONSISTENCY — missing/unparseable sidecar, or ``byte_size`` mismatch.
    * VALIDITY — zero-byte payload, or a payload declared JSON that doesn't parse.

    The sidecar's ``sha256``/``byte_size`` describe the stored bytes (identical to
    what is on disk regardless of ``stored_encoding``), so the comparison is direct.
    """
    issues: list[str] = []
    try:
        size = os.path.getsize(payload_path)
    except OSError as e:
        return [f"INTEGRITY: cannot stat payload ({e})"]

    if size == 0:
        issues.append("VALIDITY: payload is zero bytes")

    if sidecar is None:
        issues.append("CONSISTENCY: missing or unparseable sidecar")
        return issues

    declared_sha = sidecar.get("sha256")
    if declared_sha:
        actual = _sha256_file(payload_path)
        if actual != declared_sha:
            issues.append(
                f"INTEGRITY: sha256 mismatch "
                f"(sidecar {declared_sha[:12]}…, actual {actual[:12]}…)"
            )
    else:
        issues.append("CONSISTENCY: sidecar has no sha256")

    declared_size = sidecar.get("byte_size")
    if declared_size is not None and declared_size != size:
        issues.append(
            f"CONSISTENCY: byte_size mismatch (sidecar {declared_size}, actual {size})"
        )

    ctype = (sidecar.get("content_type") or "").lower()
    stored_enc = (sidecar.get("stored_encoding") or "identity").lower()
    is_json = "json" in ctype or payload_path.endswith(".json")
    if size > 0 and stored_enc == "identity" and is_json:
        try:
            with open(payload_path, "rb") as fh:
                json.loads(fh.read())
        except (OSError, json.JSONDecodeError) as e:
            issues.append(f"VALIDITY: declared JSON does not parse ({str(e)[:60]})")

    return issues


def sample_payloads(coll_dir: str, recent_partitions: int | None, sample: int) -> list[str]:
    """Up to ``sample`` payloads spread across the trailing partitions.

    Picks the first, last and an even spread between (the ``sweep_streams`` strategy)
    so a stream's health is judged across its range rather than a single corner.
    """
    files: list[str] = []
    for _dt, pdir in trailing_partition_dirs(coll_dir, recent_partitions):
        files.extend(iter_payloads(pdir))
    files.sort()
    if sample <= 0 or len(files) <= sample:
        return files
    last = len(files) - 1
    spread = {int(i * last / (sample - 1)) for i in range(sample)}
    idxs = sorted({0, last} | spread)
    return [files[i] for i in idxs][:sample]
