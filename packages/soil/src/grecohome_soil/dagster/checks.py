"""Bronze data-quality checks for the USCRN/soil code location.

One collection, ``uscrn/hourly``: raw fixed-width text rows, one daily UTC
partition. ``dt`` *is* the event date (the UTC day the rows belong to), so
completeness reads the partition dates directly. Schema-drift on text uses a simple
field-count signature (USCRN rows are space-delimited fixed columns), which catches
a format change without trying to parse JSON keys.

Freshness tolerance is a bit wider than a day because USCRN publishes the year file
with a lag and the schedule runs every 6h.
"""

from __future__ import annotations

from grecohome_core.checks import (
    CollectionCheckConfig,
    build_bronze_checks_job,
    build_bronze_checks_schedule,
    build_collection_checks,
)
from grecohome_soil.config import settings
from grecohome_soil.dagster.assets import uscrn_bronze_hourly

SOIL_CHECK_CONFIGS: list[CollectionCheckConfig] = [
    CollectionCheckConfig(
        source="uscrn",
        collection="hourly",
        asset_key=uscrn_bronze_hourly.key,
        reader="txt",
        unnest_records=False,
        event_date_source="partition",  # dt = the UTC date the rows cover
        cadence_hours=30.0,  # USCRN publish lag + 6-hourly schedule
        grace_hours=12.0,
        cadence_days=2,
    ),
]

soil_checks = build_collection_checks(
    SOIL_CHECK_CONFIGS,
    bronze_root=settings.bronze_root,
    monitor_dir=settings.bronze_monitor_dir,
)

# Checks-only job + hourly schedule (off the uscrn_api pool).
soil_checks_job = build_bronze_checks_job(soil_checks, name="uscrn_bronze_checks_job")
soil_checks_schedule = build_bronze_checks_schedule(
    soil_checks_job, name="uscrn_bronze_checks_hourly"
)
