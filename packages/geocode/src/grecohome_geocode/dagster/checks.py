"""Bronze data-quality checks for the geocode code location.

One collection (``geocode/reverse``): each payload is a raw Photon GeoJSON
``FeatureCollection``. The cache is **event-driven** (a cell is looked up once, ever), so
the API-polling checks don't apply — no new cell for weeks is normal, not stale, so
freshness/completeness are disabled. Two core checks remain:

* **Content health** (WARN) — payloads parse and carry data. A "no result" Photon
  response (``{"type":"FeatureCollection","features":[]}``) is still valid JSON with
  substantive keys, so it passes; that's correct — an empty result is a real, cacheable
  answer we don't want to re-query.
* **Schema drift** (ERROR) — the response's stable top-level shape is
  ``["features","type"]``; a change there means Photon's API contract moved. (Per-feature
  ``properties`` are polymorphic but live *inside* ``features``, so they don't perturb the
  top-level signature — no false positives.)

Checks read bronze strictly read-only and run on the checks-only job (off the geocode
pool). ``@alerting_check`` (applied by the core builders) pages only on a failing ERROR.
"""

from __future__ import annotations

from grecohome_core.checks import (
    CollectionCheckConfig,
    build_bronze_checks_job,
    build_bronze_checks_schedule,
    build_collection_checks,
)
from grecohome_geocode.capture import COLLECTION, SOURCE
from grecohome_geocode.config import settings
from grecohome_geocode.dagster.assets import geocode_bronze_reverse

GEOCODE_CHECK_CONFIGS: list[CollectionCheckConfig] = [
    CollectionCheckConfig(
        source=SOURCE,
        collection=COLLECTION,
        asset_key=geocode_bronze_reverse.key,
        reader="json",
        event_date_source="none",  # dt = lookup date; a cache has no event timeline
        enabled_checks=frozenset({"content", "schema"}),
        recent_partitions=settings.geocode_recent_partitions,
    ),
]

geocode_checks = build_collection_checks(
    GEOCODE_CHECK_CONFIGS,
    bronze_root=settings.bronze_root,
    monitor_dir=settings.bronze_monitor_dir,
)

# Checks-only job + hourly schedule (off the geocode pool).
geocode_checks_job = build_bronze_checks_job(geocode_checks, name="geocode_bronze_checks_job")
geocode_checks_schedule = build_bronze_checks_schedule(
    geocode_checks_job, name="geocode_bronze_checks_hourly"
)
