"""Content-health check: are recent payloads carrying real data, and intact?

Two concerns folded into one recent-sample pass (so we read each sampled payload
once):

* **Content** (WARN) — classify each payload (DATA / EMPTY_LIST / EMPTY_OBJECT /
  EMPTY_WRAPPER / ERROR_LIKE / HTTP_ERROR / CSV_DATA / TXT_DATA / ...). A file can
  be intact, valid JSON and still be an empty list or an error envelope. Many
  collections are *legitimately* always-empty on this hardware → mark them
  ``expected_empty`` so empties don't warn (an actual error envelope still does).
* **Integrity** (ERROR) — verify on-disk sha256/byte_size against the sidecar and
  that declared-JSON parses (the ``verify_bronze.py`` checks). Corruption outranks
  emptiness: any integrity failure makes the check ERROR.
"""

from __future__ import annotations

from collections import Counter

from dagster import (
    AssetCheckResult,
    AssetChecksDefinition,
    AssetCheckSeverity,
    asset_check,
)

from grecohome_core.checks.alerting import alerting_check
from grecohome_core.checks.bronze_reads import (
    DATA_CLASSES,
    EMPTY_CLASSES,
    classify_payload,
    collection_dir,
    read_sidecar,
    sample_payloads,
    verify_integrity,
)
from grecohome_core.checks.config import CollectionCheckConfig
from grecohome_core.logging_config import get_logger

logger = get_logger(__name__)

#: How many payloads to sample per check, spread across the trailing partitions.
DEFAULT_SAMPLE = 5

#: Non-empty, non-data classes that always warn (a real error, not just absence).
_ERROR_CLASSES = frozenset({"HTTP_ERROR", "ERROR_LIKE", "UNPARSEABLE"})


def build_content_health_check(
    cfg: CollectionCheckConfig, bronze_root: str, sample: int = DEFAULT_SAMPLE
) -> AssetChecksDefinition:
    """An ``@asset_check`` that samples recent payloads and reports content health
    (WARN on a meaningful empty/error share) and byte integrity (ERROR on
    corruption). ``expected_empty`` collections pass on empty payloads but still
    warn on genuine error envelopes.
    """

    @asset_check(
        asset=cfg.asset_key,
        name=f"{cfg.check_name_prefix}_content_health",
        blocking=False,
        description=(
            f"Recent {cfg.source}/{cfg.collection} payloads carry data and are intact."
        ),
    )
    @alerting_check(name=f"{cfg.check_name_prefix}_content_health", asset=cfg.asset_key)
    def _check() -> AssetCheckResult:
        try:
            coll_dir = collection_dir(bronze_root, cfg.source, cfg.collection)
            payloads = sample_payloads(coll_dir, cfg.recent_partitions, sample)

            if not payloads:
                # Absence of payloads is freshness's job to flag, not content's.
                passed = True
                return AssetCheckResult(
                    passed=passed,
                    severity=AssetCheckSeverity.WARN,
                    metadata={"sample_size": 0, "status": "no_payloads"},
                    description="No payloads sampled (freshness covers absence).",
                )

            verdicts: Counter[str] = Counter()
            integrity_issues: list[str] = []
            for path in payloads:
                sidecar = read_sidecar(path)
                verdicts[classify_payload(path, sidecar)] += 1
                for issue in verify_integrity(path, sidecar):
                    integrity_issues.append(f"{path.rsplit('/', 1)[-1]}: {issue}")

            counts_meta = {cls: n for cls, n in verdicts.items()}
            data_like = sum(verdicts[c] for c in verdicts if c in DATA_CLASSES)
            empties = sum(verdicts[c] for c in verdicts if c in EMPTY_CLASSES)
            errors = sum(verdicts[c] for c in verdicts if c in _ERROR_CLASSES)

            # Integrity (corruption) outranks content: ERROR.
            if integrity_issues:
                return AssetCheckResult(
                    passed=False,
                    severity=AssetCheckSeverity.ERROR,
                    metadata={
                        **counts_meta,
                        "sample_size": len(payloads),
                        "integrity_issues": len(integrity_issues),
                        "integrity_detail": "; ".join(integrity_issues[:5]),
                    },
                    description=f"{len(integrity_issues)} integrity issue(s) in sampled payloads.",
                )

            # Content emptiness/errors: WARN.
            if errors > 0:
                passed = False
                desc = f"{errors}/{len(payloads)} sampled payloads are error/unparseable."
            elif empties > 0 and not cfg.expected_empty:
                passed = False
                desc = f"{empties}/{len(payloads)} sampled payloads are empty."
            elif empties > 0 and cfg.expected_empty:
                passed = True
                desc = f"{empties}/{len(payloads)} empty (expected-empty collection)."
            else:
                passed = True
                desc = f"{data_like}/{len(payloads)} sampled payloads carry data."

            return AssetCheckResult(
                passed=passed,
                severity=AssetCheckSeverity.WARN,
                metadata={
                    **counts_meta,
                    "sample_size": len(payloads),
                    "data_like": data_like,
                    "empty": empties,
                    "error": errors,
                    "expected_empty": cfg.expected_empty,
                },
                description=desc,
            )
        except Exception as e:  # noqa: BLE001 - a check must never break the run
            logger.warning(
                "content-health check errored",
                source=cfg.source,
                collection=cfg.collection,
                error=str(e),
            )
            return AssetCheckResult(
                passed=False,
                severity=AssetCheckSeverity.WARN,
                metadata={"error": str(e)},
                description="Content-health check errored internally.",
            )

    return _check
