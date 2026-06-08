"""Atomic plaintext-JSON token store.

A small, source-agnostic store for OAuth token state persisted to a mounted,
writable file (in production a secrets-manager / Ansible-managed path). Writes
are atomic (temp file + ``os.replace``) so a crash mid-write never corrupts the
file or loses a rotated refresh token.

This deliberately stores plaintext: encryption-at-rest is provided by the mounted
volume / secrets manager, not by the application.
"""

import json
import os
import secrets
from typing import Any

from grecohome_core.logging_config import get_logger

logger = get_logger(__name__)


class TokenFileStore:
    """Read/write OAuth token state as a single JSON file.

    Args:
        path: Absolute path to the token JSON file.
    """

    def __init__(self, path: str) -> None:
        self.path = path

    def read(self) -> dict[str, Any] | None:
        """Return the stored token dict, or ``None`` if the file does not exist.

        A malformed file raises ``json.JSONDecodeError`` so the caller can decide
        how to handle corruption rather than silently treating it as "no token".
        """
        if not os.path.exists(self.path):
            return None
        with open(self.path, encoding="utf-8") as fh:
            return json.load(fh)

    def write_atomic(self, data: dict[str, Any]) -> None:
        """Atomically write ``data`` as JSON to the token path.

        The temp file is created in the same directory so the rename stays on one
        filesystem; the directory is created if needed.
        """
        directory = os.path.dirname(self.path) or "."
        os.makedirs(directory, exist_ok=True)
        tmp_path = os.path.join(directory, f".tmp_{secrets.token_hex(8)}")
        try:
            with open(tmp_path, "w", encoding="utf-8") as fh:
                json.dump(data, fh, separators=(",", ":"), sort_keys=True)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp_path, self.path)
        except Exception:
            try:
                os.remove(tmp_path)
            except OSError:
                pass
            raise
        logger.debug("token file written", path=self.path)
