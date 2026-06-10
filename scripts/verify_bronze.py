#!/usr/bin/env python3
"""
verify_bronze.py — Foundation check for the bronze layer.

Verifies four properties against a bronze tree of payloads + .meta.json sidecars:

  INTEGRITY    stored bytes match the sidecar sha256 (no write corruption)
  VALIDITY     payload parses as its declared content_type and is non-empty
  CONSISTENCY  sidecar fields agree with reality (byte_size, sha present, pairing)
  COMPLETENESS no unexplained gaps in the per-day capture sequence

Read-only. Never writes to or mutates the bronze tree. Safe to re-run anytime
(e.g. after onboarding a new source). Exit code 0 = clean, 1 = problems found.

Usage:
    python3 verify_bronze.py /opt/datalake/bronze
    python3 verify_bronze.py /opt/datalake/bronze --max-gap 1 --json report.json
"""
import argparse, hashlib, json, os, sys, datetime as dt
from collections import defaultdict

META_SUFFIX = ".meta.json"


def find_payloads(root):
    """Yield (payload_path, meta_path|None) for every non-sidecar .* file."""
    for dirpath, _, files in os.walk(root):
        for name in files:
            if name.endswith(META_SUFFIX):
                continue
            payload = os.path.join(dirpath, name)
            # sidecar is "<payload-without-ext>.meta.json" OR "<payload>.meta.json"
            stem, _ = os.path.splitext(payload)
            cand1 = stem + META_SUFFIX           # recovery_..._id.meta.json
            cand2 = payload + META_SUFFIX        # recovery_..._id.json.meta.json
            meta = cand1 if os.path.exists(cand1) else (cand2 if os.path.exists(cand2) else None)
            yield payload, meta


def parse_path(payload, root):
    """Extract source / collection / dt= partition from the path layout."""
    rel = os.path.relpath(payload, root)
    parts = rel.split(os.sep)
    source = parts[0] if len(parts) > 0 else None
    collection = parts[1] if len(parts) > 1 else None
    day = None
    for p in parts:
        if p.startswith("dt="):
            day = p[3:]
            break
    return source, collection, day


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def check_one(payload, meta_path):
    """Run integrity/validity/consistency on a single capture. Returns list of issues."""
    issues = []
    size = os.path.getsize(payload)

    # --- consistency: sidecar must exist and pair ---
    if meta_path is None:
        issues.append(("CONSISTENCY", "missing sidecar (.meta.json) for payload"))
        meta = {}
    else:
        try:
            with open(meta_path) as f:
                meta = json.load(f)
        except Exception as e:
            issues.append(("CONSISTENCY", f"sidecar unparseable: {e}"))
            meta = {}

    # --- validity: non-empty ---
    if size == 0:
        issues.append(("VALIDITY", "payload is zero bytes"))

    # --- integrity: sha256 match ---
    declared_sha = meta.get("sha256")
    if declared_sha:
        actual = sha256_file(payload)
        if actual != declared_sha:
            issues.append(("INTEGRITY",
                           f"sha256 mismatch (sidecar {declared_sha[:12]}…, actual {actual[:12]}…)"))
    elif meta:
        issues.append(("CONSISTENCY", "sidecar has no sha256 field"))

    # --- consistency: byte_size match ---
    declared_size = meta.get("byte_size")
    if declared_size is not None and declared_size != size:
        issues.append(("CONSISTENCY",
                       f"byte_size mismatch (sidecar {declared_size}, actual {size})"))

    # --- validity: parses as declared type (JSON only for now) ---
    ctype = (meta.get("content_type") or "").lower()
    stored_enc = (meta.get("stored_encoding") or "identity").lower()
    if size > 0 and stored_enc == "identity" and ("json" in ctype or payload.endswith(".json")):
        try:
            with open(payload, "rb") as f:
                json.loads(f.read())
        except Exception as e:
            issues.append(("VALIDITY", f"declared JSON does not parse: {str(e)[:60]}"))

    # --- consistency: redaction visibility ---
    if meta.get("redacted_fields"):
        issues.append(("CONSISTENCY",
                       f"payload had fields redacted: {meta['redacted_fields']} (informational)"))

    return issues, meta


def check_completeness(days_by_stream, max_gap):
    """Flag gaps larger than max_gap days within each source/collection's date range."""
    findings = []
    for (source, collection), days in sorted(days_by_stream.items()):
        valid = sorted(d for d in days if d)
        if len(valid) < 2:
            continue
        try:
            dates = [dt.date.fromisoformat(d) for d in valid]
        except ValueError:
            findings.append((source, collection, "non-ISO dt= partition names present"))
            continue
        for a, b in zip(dates, dates[1:]):
            gap = (b - a).days
            if gap > max_gap + 1:
                missing = gap - 1
                findings.append((source, collection,
                                 f"{missing}-day gap between {a} and {b}"))
    return findings


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("root", help="bronze root directory")
    ap.add_argument("--max-gap", type=int, default=0,
                    help="allowed gap in days between consecutive capture days before flagging (default 0)")
    ap.add_argument("--json", help="optional path to write a machine-readable report")
    args = ap.parse_args()

    if not os.path.isdir(args.root):
        print(f"ERROR: not a directory: {args.root}", file=sys.stderr)
        sys.exit(2)

    per_file_issues = []      # (payload, source, collection, day, [(class, msg)])
    days_by_stream = defaultdict(set)
    counts_by_stream = defaultdict(int)
    total = 0

    for payload, meta_path in find_payloads(args.root):
        total += 1
        source, collection, day = parse_path(payload, args.root)
        days_by_stream[(source, collection)].add(day)
        counts_by_stream[(source, collection)] += 1
        issues, _ = check_one(payload, meta_path)
        if issues:
            per_file_issues.append((payload, source, collection, day, issues))

    completeness = check_completeness(days_by_stream, args.max_gap)

    # ---------- report ----------
    print("=" * 72)
    print(f"BRONZE VERIFICATION  —  {args.root}")
    print(f"scanned {total} payload file(s) across {len(counts_by_stream)} stream(s)")
    print("=" * 72)

    print("\nINVENTORY (source / collection → files, day span)")
    for (s, c), n in sorted(counts_by_stream.items()):
        days = sorted(d for d in days_by_stream[(s, c)] if d)
        span = f"{days[0]} … {days[-1]}" if days else "(no dt= partitions)"
        print(f"  {s}/{c}: {n} files, {len(days)} days, {span}")

    # group file issues by class
    by_class = defaultdict(list)
    for payload, s, c, day, issues in per_file_issues:
        for cls, msg in issues:
            by_class[cls].append((os.path.relpath(payload, args.root), msg))

    problem_classes = ["INTEGRITY", "VALIDITY", "CONSISTENCY"]
    any_hard = False
    for cls in problem_classes:
        items = by_class.get(cls, [])
        # "informational" consistency lines aren't hard failures
        hard = [(p, m) for p, m in items if "informational" not in m]
        if items:
            print(f"\n{cls}  ({len(items)} finding(s))")
            for p, m in items:
                print(f"  - {p}\n      {m}")
        if hard:
            any_hard = True

    print(f"\nCOMPLETENESS  ({len(completeness)} gap finding(s), --max-gap={args.max_gap})")
    if completeness:
        for s, c, msg in completeness:
            print(f"  - {s}/{c}: {msg}")
        print("  (gaps may be legitimate — e.g. device not worn. Review, don't assume corruption.)")
    else:
        print("  no gaps beyond allowed threshold")

    print("\n" + "=" * 72)
    if any_hard:
        print("RESULT: PROBLEMS FOUND — review INTEGRITY/VALIDITY/CONSISTENCY above.")
    else:
        print("RESULT: foundation looks sound (no integrity/validity/consistency failures).")
    print("=" * 72)

    if args.json:
        report = {
            "root": args.root, "scanned": total,
            "inventory": {f"{s}/{c}": {"files": n,
                          "days": sorted(d for d in days_by_stream[(s, c)] if d)}
                          for (s, c), n in counts_by_stream.items()},
            "file_issues": [{"file": os.path.relpath(p, args.root), "source": s,
                             "collection": c, "day": day,
                             "issues": [{"class": cl, "msg": m} for cl, m in iss]}
                            for p, s, c, day, iss in per_file_issues],
            "completeness": [{"source": s, "collection": c, "finding": m}
                             for s, c, m in completeness],
        }
        with open(args.json, "w") as f:
            json.dump(report, f, indent=2)
        print(f"\nmachine-readable report written to {args.json}")

    sys.exit(1 if any_hard else 0)


if __name__ == "__main__":
    main()