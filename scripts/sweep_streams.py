#!/usr/bin/env python3
"""
sweep_streams.py — Content-level health sweep across ALL bronze streams.

The verifier checks integrity/validity/consistency (are bytes intact, does JSON
parse, do sidecars match). This is different: it checks whether payloads actually
CARRY DATA. A file can pass verification — intact, valid JSON, correct sidecar —
and still be an empty list, an empty records wrapper, or an error envelope.

For every source/collection it samples the payloads and classifies each as:
  DATA          non-empty content that looks like real records
  EMPTY_LIST    payload is []
  EMPTY_OBJECT  payload is {}
  EMPTY_WRAPPER payload is {"records": []} (or similar empty data array)
  ERROR_LIKE    payload looks like an error envelope (has error/message, no data)
  HTTP_ERROR    sidecar reports non-2xx http_status
  CSV_DATA      non-JSON (e.g. CSV) with content (reported, not parsed here)

Then it summarizes which streams have NO usable data, which are mixed, and which
are fully healthy — so you get the complete list of streams to investigate in one
pass, not one at a time.

Read-only. Usage:
    python3 sweep_streams.py /opt/datalake/bronze
    python3 sweep_streams.py /opt/datalake/bronze --sample 5   # files sampled per stream
"""
import argparse, json, os, sys, glob
from collections import defaultdict, Counter

META = ".meta.json"
DATA_ARRAY_KEYS = ("records", "data", "items", "results", "activities")
ERROR_KEYS = ("error", "errors", "message", "fault", "exception", "status_code")


def classify_json(obj):
    if isinstance(obj, list):
        return "EMPTY_LIST" if len(obj) == 0 else "DATA"
    if isinstance(obj, dict):
        if len(obj) == 0:
            return "EMPTY_OBJECT"
        # empty data-array wrapper?
        for k in DATA_ARRAY_KEYS:
            if k in obj and isinstance(obj[k], list) and len(obj[k]) == 0:
                # but only "empty" if there's no other substantive payload
                others = [kk for kk in obj if kk not in (k, "next_token", "nextToken", "next", "paging")]
                if not others:
                    return "EMPTY_WRAPPER"
        # error envelope? (error-ish keys present AND no data array with content)
        lower = {k.lower() for k in obj}
        has_error = any(e in lower for e in ERROR_KEYS)
        has_data_array = any(isinstance(obj.get(k), list) and len(obj[k]) > 0 for k in DATA_ARRAY_KEYS)
        if has_error and not has_data_array:
            # 'status' alone is ambiguous; require a real error-ish key
            if any(e in lower for e in ("error", "errors", "fault", "exception")):
                return "ERROR_LIKE"
        return "DATA"
    return "DATA"  # scalar — unusual but not empty


def sample_files(stream_dir, n):
    files = [f for f in glob.glob(os.path.join(stream_dir, "dt=*", "*"))
             if not f.endswith(META)]
    files.sort()
    # sample across the range: first, last, and some middle
    if len(files) <= n:
        return files
    idxs = sorted(set([0, len(files) - 1] + [int(i * (len(files) - 1) / (n - 1)) for i in range(n)]))
    return [files[i] for i in idxs][:n]


def sidecar_status(payload_path):
    stem, _ = os.path.splitext(payload_path)
    for cand in (stem + META, payload_path + META):
        if os.path.exists(cand):
            try:
                with open(cand) as f:
                    return json.load(f).get("http_status")
            except Exception:
                return None
    return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("root")
    ap.add_argument("--sample", type=int, default=5, help="files sampled per stream")
    args = ap.parse_args()

    # discover streams: {root}/{source}/{collection}/dt=.../
    streams = {}
    for source in sorted(os.listdir(args.root)):
        sp = os.path.join(args.root, source)
        if not os.path.isdir(sp):
            continue
        for collection in sorted(os.listdir(sp)):
            cp = os.path.join(sp, collection)
            if os.path.isdir(cp):
                streams[f"{source}/{collection}"] = cp

    results = {}
    total_files = defaultdict(int)
    for stream, path in streams.items():
        all_files = [f for f in glob.glob(os.path.join(path, "dt=*", "*")) if not f.endswith(META)]
        total_files[stream] = len(all_files)
        sampled = sample_files(path, args.sample)
        verdicts = Counter()
        for fp in sampled:
            status = sidecar_status(fp)
            if status is not None and not (200 <= int(status) < 300):
                verdicts["HTTP_ERROR"] += 1
                continue
            if not fp.endswith(".json"):
                size = os.path.getsize(fp)
                verdicts["CSV_DATA" if size > 0 else "EMPTY_FILE"] += 1
                continue
            try:
                with open(fp, "rb") as f:
                    obj = json.loads(f.read() or "null")
                verdicts[classify_json(obj) if obj is not None else "EMPTY_FILE"] += 1
            except Exception:
                verdicts["UNPARSEABLE"] += 1
        results[stream] = verdicts

    # ---- report ----
    def stream_state(v):
        data_like = v.get("DATA", 0) + v.get("CSV_DATA", 0)
        bad = sum(v[k] for k in v if k not in ("DATA", "CSV_DATA"))
        if data_like and not bad:
            return "HEALTHY"
        if data_like and bad:
            return "MIXED"
        return "NO_DATA"

    grouped = defaultdict(list)
    for stream, v in results.items():
        grouped[stream_state(v)].append((stream, v))

    print("=" * 72)
    print(f"STREAM CONTENT SWEEP — {args.root}")
    print(f"{len(results)} streams, sampling up to {args.sample} files each")
    print("=" * 72)

    for state, header in [("NO_DATA", "STREAMS WITH NO USABLE DATA — investigate these"),
                          ("MIXED", "MIXED STREAMS — some empty/error payloads"),
                          ("HEALTHY", "HEALTHY STREAMS — carrying data")]:
        items = sorted(grouped.get(state, []))
        print(f"\n{header}  ({len(items)})")
        if not items:
            print("  none")
            continue
        for stream, v in items:
            detail = ", ".join(f"{k}:{c}" for k, c in v.most_common())
            print(f"  {stream}  (n={total_files[stream]} files)  [{detail}]")

    nd = len(grouped.get("NO_DATA", []))
    print("\n" + "=" * 72)
    print(f"SUMMARY: {len(grouped.get('HEALTHY',[]))} healthy, "
          f"{len(grouped.get('MIXED',[]))} mixed, {nd} no-data")
    print("The no-data and mixed streams are your full investigation list.")
    print("=" * 72)


if __name__ == "__main__":
    main()