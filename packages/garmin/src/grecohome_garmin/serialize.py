"""Deterministic JSON serialization and secret-screening for Garmin captures.

Two small, pure helpers:

1. ``to_bronze_json`` — turn a parsed JSON object back into bytes for the
   ``reserialized`` capture grade. Compact, ``ensure_ascii=False``, and *no*
   ``sort_keys`` (preserve the library's key order).
2. ``screen_for_secrets`` — the one permitted bronze payload modification.
   Applied only to endpoints flagged for it (profile/settings), it recursively
   removes any key whose name matches a secret pattern and returns the redacted
   key paths for the sidecar's ``redacted_fields``.
"""

import json
import re
from typing import Any

# Keys whose *name* indicates a credential/secret. Matched case-insensitively as
# a substring. Deliberately broad: bronze is immutable and replicated, so
# over-redaction is cheap and under-redaction is forbidden.
_SECRET_KEY_PATTERNS: tuple[str, ...] = (
    "password",
    "passwd",
    "secret",
    "token",
    "accesstoken",
    "refreshtoken",
    "idtoken",
    "apikey",
    "api_key",
    "authorization",
    "auth_token",
    "authtoken",
    "credential",
    "private_key",
    "privatekey",
    "client_secret",
    "clientsecret",
    "signature",
    "sessionid",
    "session_id",
)

_SECRET_KEY_RE = re.compile("|".join(re.escape(p) for p in _SECRET_KEY_PATTERNS), re.IGNORECASE)


def to_bronze_json(parsed: Any) -> bytes:
    """Serialize a parsed JSON object to deterministic bronze bytes.

    Compact separators, ``ensure_ascii=False`` (UTF-8 text stays UTF-8), and *no*
    ``sort_keys`` -- reordering keys would be a reshaping we explicitly avoid.
    """
    return json.dumps(parsed, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def _key_is_secret(key: Any) -> bool:
    return isinstance(key, str) and bool(_SECRET_KEY_RE.search(key))


def screen_for_secrets(obj: Any, *, path: str = "") -> tuple[Any, list[str]]:
    """Recursively remove secret-looking keys from a parsed JSON object.

    Returns ``(screened_object, redacted_field_paths)``; the input is not
    mutated. Paths are dotted/indexed (e.g. ``profile.accessToken``) so the
    sidecar records exactly what was dropped.
    """
    redacted: list[str] = []

    def _walk(node: Any, node_path: str) -> Any:
        if isinstance(node, dict):
            out: dict[Any, Any] = {}
            for key, value in node.items():
                child_path = f"{node_path}.{key}" if node_path else str(key)
                if _key_is_secret(key):
                    redacted.append(child_path)
                    continue
                out[key] = _walk(value, child_path)
            return out
        if isinstance(node, list):
            return [_walk(item, f"{node_path}[{idx}]") for idx, item in enumerate(node)]
        return node

    screened = _walk(obj, path)
    return screened, redacted
