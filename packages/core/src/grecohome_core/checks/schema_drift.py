"""Schema-drift check: has the payload's top-level shape changed from baseline?

A stable signature (sorted top-level keys / CSV columns / txt field-count) is
computed from **one exact payload file** — never a glob — so sidecar fields can't
leak into it. The signature is compared to a stored baseline; the first time a
collection is seen the current signature *becomes* the baseline (pass). A change
is an ERROR: the source contract or our capture moved under us.

**Baselines live OUTSIDE ``BRONZE_ROOT``** (under ``bronze_monitor_dir``) so the
bronze tree stays immutable raw capture. The check refuses to write a baseline
that would land inside the bronze root.
"""

from __future__ import annotations

import json
import os
import secrets
from datetime import UTC, datetime

from dagster import (
    AssetCheckResult,
    AssetChecksDefinition,
    AssetCheckSeverity,
    asset_check,
)

from grecohome_core.checks.bronze_reads import collection_dir, schema_signature
from grecohome_core.checks.config import CollectionCheckConfig
from grecohome_core.logging_config import get_logger

logger = get_logger(__name__)


def _baseline_path(monitor_dir: str, source: str, collection: str) -> str:
    return os.path.join(monitor_dir, "schema_baselines", source, f"{collection}.json")


def _is_within(path: str, root: str) -> bool:
    """True if ``path`` resolves inside ``root`` (used to refuse bronze writes)."""
    abs_root = os.path.abspath(root)
    try:
        return os.path.commonpath([os.path.abspath(path), abs_root]) == abs_root
    except ValueError:  # different drives on Windows, etc.
        return False


def _read_baseline(path: str) -> list[str] | None:
    try:
        with open(path) as fh:
            data = json.load(fh)
        sig = data.get("signature")
        return list(sig) if isinstance(sig, list) else None
    except (OSError, json.JSONDecodeError):
        return None


def _write_baseline(path: str, signature: list[str], source: str, collection: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    payload = json.dumps(
        {
            "source": source,
            "collection": collection,
            "signature": signature,
            "recorded_at": datetime.now(UTC).isoformat(),
        },
        indent=2,
    ).encode("utf-8")
    tmp = path + f".tmp_{secrets.token_hex(6)}"
    with open(tmp, "wb") as fh:
        fh.write(payload)
    os.replace(tmp, path)


def build_schema_drift_check(
    cfg: CollectionCheckConfig, bronze_root: str, monitor_dir: str | None
) -> AssetChecksDefinition:
    """An ``@asset_check`` that fails (ERROR) when the payload signature differs from
    the recorded baseline. The first observation records the baseline and passes;
    when no ``monitor_dir`` is configured the check passes with a ``disabled`` note
    (it cannot persist state, and bronze must never hold it).
    """

    @asset_check(
        asset=cfg.asset_key,
        name=f"{cfg.check_name_prefix}_schema_drift",
        blocking=False,
        description=(
            f"Top-level shape of {cfg.source}/{cfg.collection} payloads matches the "
            "recorded baseline."
        ),
    )
    def _check() -> AssetCheckResult:
        try:
            coll_dir = collection_dir(bronze_root, cfg.source, cfg.collection)
            now_sig = schema_signature(
                coll_dir,
                reader=cfg.reader,
                unnest_records=cfg.unnest_records,
                recent_partitions=cfg.recent_partitions,
            )

            if now_sig is None:
                return AssetCheckResult(
                    passed=True,
                    severity=AssetCheckSeverity.ERROR,
                    metadata={"status": "no_payload", "was": "", "now": ""},
                    description="No payload yet to compute a schema signature.",
                )

            if monitor_dir is None:
                return AssetCheckResult(
                    passed=True,
                    severity=AssetCheckSeverity.ERROR,
                    metadata={"status": "disabled", "was": "", "now": json.dumps(now_sig)},
                    description="Schema drift disabled: BRONZE_MONITOR_DIR is not set.",
                )

            baseline_file = _baseline_path(monitor_dir, cfg.source, cfg.collection)
            if _is_within(baseline_file, bronze_root):
                raise RuntimeError(
                    "refusing to write schema baseline inside BRONZE_ROOT "
                    f"({baseline_file}); set BRONZE_MONITOR_DIR outside bronze"
                )

            baseline = _read_baseline(baseline_file)
            if baseline is None:
                _write_baseline(baseline_file, now_sig, cfg.source, cfg.collection)
                return AssetCheckResult(
                    passed=True,
                    severity=AssetCheckSeverity.ERROR,
                    metadata={"status": "baseline_set", "was": "", "now": json.dumps(now_sig)},
                    description=f"Baseline recorded: {len(now_sig)} top-level field(s).",
                )

            if baseline == now_sig:
                return AssetCheckResult(
                    passed=True,
                    severity=AssetCheckSeverity.ERROR,
                    metadata={
                        "status": "ok",
                        "was": json.dumps(baseline),
                        "now": json.dumps(now_sig),
                    },
                    description="Schema matches baseline.",
                )

            return AssetCheckResult(
                passed=False,
                severity=AssetCheckSeverity.ERROR,
                metadata={
                    "status": "drift",
                    "was": json.dumps(baseline),
                    "now": json.dumps(now_sig),
                    "added": json.dumps(sorted(set(now_sig) - set(baseline))),
                    "removed": json.dumps(sorted(set(baseline) - set(now_sig))),
                },
                description="Schema drift: payload top-level shape changed from baseline.",
            )
        except Exception as e:  # noqa: BLE001 - a check must never break the run
            logger.warning(
                "schema-drift check errored",
                source=cfg.source,
                collection=cfg.collection,
                error=str(e),
            )
            return AssetCheckResult(
                passed=False,
                severity=AssetCheckSeverity.ERROR,
                metadata={"status": "error", "error": str(e)},
                description="Schema-drift check errored internally.",
            )

    return _check
