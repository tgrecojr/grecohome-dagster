#!/usr/bin/env python3
"""Fetch Garmin's Personal API OpenAPI spec and diff it against the committed snapshot.

Garmin publishes the spec only inside the JavaScript bundle behind its Swagger page, and the
spec's ``info.version`` does not change when endpoints are added. So a re-evaluation of
``docs/intent/garmin/decisions/personal-api.md`` compares operations, not version numbers.

Usage (stdlib only, no venv needed):

    python3 scripts/garmin_personal_api_spec.py            # diff live spec vs the snapshot
    python3 scripts/garmin_personal_api_spec.py --write    # overwrite the snapshot

Exit status: 0 when nothing changed, 1 when the live spec differs from the snapshot.
"""

from __future__ import annotations

import argparse
import codecs
import json
import re
import sys
import urllib.request
from pathlib import Path

SITE = "https://developer.garmin.com"
SWAGGER_PAGE = f"{SITE}/gc-developer-program/personal/swagger-docs/"
SNAPSHOT = (
    Path(__file__).resolve().parent.parent
    / "docs/intent/garmin/decisions/personal-api-openapi.json"
)
_MARKER = "JSON.parse('{\"openapi\""


def _get(url: str) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=60) as resp:
        return resp.read().decode("utf-8")


def fetch_live_spec() -> dict:
    """Find the Swagger page's component bundle and decode the embedded spec literal."""
    page = _get(SWAGGER_PAGE)
    bundle = re.search(r'src="(/component---[^"]*swagger-docs[^"]*\.js)"', page)
    if not bundle:
        sys.exit(f"no swagger-docs bundle found on {SWAGGER_PAGE}")
    js = _get(SITE + bundle.group(1))
    start = js.find(_MARKER)
    if start < 0:
        sys.exit("no embedded OpenAPI document found in the bundle")
    start += len("JSON.parse('")
    end = start
    while js[end] != "'":  # the JS single-quoted literal ends at the first unescaped quote
        end += 2 if js[end] == "\\" else 1
    literal = js[start:end].encode("latin-1", "backslashreplace")
    return json.loads(codecs.decode(literal, "unicode_escape"))


def operations(spec: dict) -> dict[str, dict]:
    """Map ``METHOD /path`` to that operation's definition."""
    return {
        f"{method.upper()} {path}": op
        for path, methods in spec.get("paths", {}).items()
        for method, op in methods.items()
    }


def diff(old: dict, new: dict) -> list[str]:
    old_ops, new_ops = operations(old), operations(new)
    lines = [f"+ {k}" for k in sorted(new_ops.keys() - old_ops.keys())]
    lines += [f"- {k}" for k in sorted(old_ops.keys() - new_ops.keys())]
    lines += [
        f"~ {k}" for k in sorted(old_ops.keys() & new_ops.keys()) if old_ops[k] != new_ops[k]
    ]
    old_s = old.get("components", {}).get("schemas", {})
    new_s = new.get("components", {}).get("schemas", {})
    lines += [f"+ schema {k}" for k in sorted(new_s.keys() - old_s.keys())]
    lines += [f"- schema {k}" for k in sorted(old_s.keys() - new_s.keys())]
    lines += [f"~ schema {k}" for k in sorted(old_s.keys() & new_s.keys()) if old_s[k] != new_s[k]]
    return lines


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--write", action="store_true", help="overwrite the committed snapshot")
    args = parser.parse_args()

    live = fetch_live_spec()
    print(f"live: {len(operations(live))} operations, info.version {live['info']['version']}")
    if args.write:
        SNAPSHOT.write_text(json.dumps(live, indent=1, sort_keys=True, ensure_ascii=False) + "\n")
        print(f"wrote {SNAPSHOT}")
        return 0

    changes = diff(json.loads(SNAPSHOT.read_text()), live)
    print("\n".join(changes) if changes else "no changes since the snapshot")
    return 1 if changes else 0


if __name__ == "__main__":
    sys.exit(main())
