"""Bronze data-quality checks for the Lingo (CGM glucose) code location.

One collection, ``lingo/glucose``, captured as raw CSV. Two Lingo-specific facts
shape the checks:

* **``dt`` is the *fetch* date, not the event date.** Each export is a cumulative
  CSV; the real reading time lives *inside* the file. Completeness therefore parses
  the in-CSV timestamp column (confirmed against the live tree:
  ``Time of Glucose Reading [T=(local time) +/- (time zone offset)]``, values like
  ``2026-06-05T21:45-04:00``). Overlapping rows across exports collapse to DISTINCT
  event dates.
* **Wear is intermittent** (the CGM is worn in bursts), so freshness is *disabled*:
  "no new upload in a while" is usually legitimate non-wear, not a broken pipeline,
  and an ERROR there would cry wolf. Gaps are surfaced by completeness as WARN with
  a wide tolerance instead. Schema + content still apply.

A bespoke ``lingo_drive_folder_health`` check covers what freshness can't: a wrong,
empty or un-shared folder makes Drive return an empty listing (not an error), so the
sensor skips silently forever. The folder only accumulates exports, so an empty
listing -- or a listing error -- is always a fault, and the check pages for it.
"""

from __future__ import annotations

from dagster import AssetCheckResult, AssetCheckSeverity, asset_check

from grecohome_core.checks import (
    CollectionCheckConfig,
    alerting_check,
    build_bronze_checks_job,
    build_bronze_checks_schedule,
    build_collection_checks,
)
from grecohome_core.logging_config import get_logger
from grecohome_lingo import drive
from grecohome_lingo.config import settings
from grecohome_lingo.dagster.assets import lingo_bronze_glucose

# The reading-timestamp column header, verified against the live bronze CSVs.
GLUCOSE_TS_COLUMN = "Time of Glucose Reading [T=(local time) +/- (time zone offset)]"

logger = get_logger(__name__)

# @spec LINGO-CHECKS-001
LINGO_CHECK_CONFIGS: list[CollectionCheckConfig] = [
    CollectionCheckConfig(
        source="lingo",
        collection="glucose",
        asset_key=lingo_bronze_glucose.key,
        reader="csv",
        unnest_records=False,
        event_date_source="payload",
        event_date_field=GLUCOSE_TS_COLUMN,
        cadence_days=14,  # wide: intermittent wear, gaps are expected → WARN only
        # Freshness off: sensor-driven, irregular uploads make sidecar-freshness a
        # false-alarm signal here.
        enabled_checks=frozenset({"completeness", "schema", "content"}),
    ),
]



# @spec LINGO-CHECKS-002, LINGO-CHECKS-003
@asset_check(asset=lingo_bronze_glucose, name="lingo_drive_folder_health")
@alerting_check
def lingo_drive_folder_health() -> AssetCheckResult:
    """ERROR if the watched Drive folder can't be listed or holds no CSV files.

    Reports only the folder id, the file count, or the error's type name -- never the
    key path, credentials, or the error text (which can quote the key file).
    """
    metadata: dict = {"folder_id": settings.gdrive_folder_id}
    try:
        files = drive.list_csv_files(drive.get_drive_service())
    except Exception as e:  # noqa: BLE001 - a failed listing is the signal, not a crash
        metadata["error_type"] = type(e).__name__
        logger.warning("lingo_drive_folder_unhealthy", **metadata)
        return AssetCheckResult(passed=False, severity=AssetCheckSeverity.ERROR, metadata=metadata)
    metadata["csv_files"] = len(files)
    passed = len(files) > 0
    if not passed:
        logger.warning("lingo_drive_folder_unhealthy", **metadata)
    return AssetCheckResult(passed=passed, severity=AssetCheckSeverity.ERROR, metadata=metadata)


lingo_checks = [
    *build_collection_checks(
        LINGO_CHECK_CONFIGS,
        bronze_root=settings.bronze_root,
        monitor_dir=settings.bronze_monitor_dir,
    ),
    lingo_drive_folder_health,
]

# Checks-only job + hourly schedule (off the lingo_api pool). Minute 23 is this code
# location's own off-minute (see docs/DEPLOYMENT.md for the others).
# @spec LINGO-CHECKS-001
lingo_checks_job = build_bronze_checks_job(lingo_checks, name="lingo_bronze_checks_job")
lingo_checks_schedule = build_bronze_checks_schedule(
    lingo_checks_job, name="lingo_bronze_checks_hourly", cron_schedule="23 * * * *"
)
