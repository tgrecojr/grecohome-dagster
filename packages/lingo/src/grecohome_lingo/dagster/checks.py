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
"""

from __future__ import annotations

from grecohome_core.checks import (
    CollectionCheckConfig,
    build_bronze_checks_job,
    build_bronze_checks_schedule,
    build_collection_checks,
)
from grecohome_lingo.config import settings
from grecohome_lingo.dagster.assets import lingo_bronze_glucose

# The reading-timestamp column header, verified against the live bronze CSVs.
GLUCOSE_TS_COLUMN = "Time of Glucose Reading [T=(local time) +/- (time zone offset)]"

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

lingo_checks = build_collection_checks(
    LINGO_CHECK_CONFIGS,
    bronze_root=settings.bronze_root,
    monitor_dir=settings.bronze_monitor_dir,
)

# Checks-only job + hourly schedule (off the lingo_api pool).
lingo_checks_job = build_bronze_checks_job(lingo_checks, name="lingo_bronze_checks_job")
lingo_checks_schedule = build_bronze_checks_schedule(
    lingo_checks_job, name="lingo_bronze_checks_hourly"
)
