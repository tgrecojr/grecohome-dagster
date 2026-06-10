"""Shared fixtures for the bronze-check tests.

A synthetic bronze tree is built with the *real* ``capture_bronze`` writer under a
frozen clock, so payloads carry authentic sidecars (sha256, byte_size, fetched_at,
and a ``.meta.json`` beside every payload). That makes the sidecar-exclusion logic
genuinely exercised — the bug class most likely to regress.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import UTC, datetime

import pytest
from freezegun import freeze_time

from grecohome_core.bronze.capture import capture_bronze

# A capture factory: write one payload+sidecar at a chosen fetch time/partition.
CaptureFn = Callable[..., str | None]


@pytest.fixture
def bronze_root(tmp_path) -> str:
    """An isolated bronze root for one test."""
    root = tmp_path / "bronze"
    root.mkdir()
    return str(root)


@pytest.fixture
def capture(bronze_root: str) -> CaptureFn:
    """Return a function that writes a capture at a given fetch time and partition.

    Signature:
        capture(source, collection, body, *, dt=None, fetched="2024-12-01T12:00:00",
                content_type="application/json", ext=None, dedupe=False) -> payload path

    ``body`` may be a dict/list (JSON-encoded), str, or bytes. ``fetched`` (ISO,
    UTC-naive) sets both the sidecar ``fetched_at`` and the filename millis, so
    freshness/ordering are deterministic.
    """

    def _capture(
        source: str,
        collection: str,
        body,
        *,
        dt: str | None = None,
        fetched: str = "2024-12-01T12:00:00",
        content_type: str = "application/json",
        ext: str | None = None,
        dedupe: bool = False,
    ) -> str | None:
        if isinstance(body, (dict, list)):
            raw = json.dumps(body).encode("utf-8")
        elif isinstance(body, str):
            raw = body.encode("utf-8")
        else:
            raw = body
        with freeze_time(fetched):
            return capture_bronze(
                source,
                collection,
                raw,
                {"content_type": content_type, "http_status": 200},
                bronze_root=bronze_root,
                dt=dt,
                dedupe=dedupe,
                ext=ext,
            )

    return _capture


@pytest.fixture
def frozen_now() -> Callable[[str], object]:
    """Convenience: ``with frozen_now("2024-12-10T12:00:00"):`` to control now()."""
    return lambda iso: freeze_time(iso)


def at(iso: str) -> datetime:
    """Parse a UTC-naive ISO string to an aware UTC datetime (test helper)."""
    return datetime.fromisoformat(iso).replace(tzinfo=UTC)
