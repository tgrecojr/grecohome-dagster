"""Bronze data-quality checks for the Garmin code location.

Garmin has ~60 catalog collections, so the configs are **generated from the
catalog** (the same registry that drives capture) rather than hand-listed — the
catalog is the source of truth, including which collections are legitimately empty.

Policy (kept lightweight given the collection count):

* **Freshness on every collection** — the one universal "are we still capturing
  this?" signal. The Garmin daily/reference schedules run once a day, so the
  tolerance is a day-plus.
* **Completeness + schema + content** only on the analytically-important daily
  time-series (:data:`IMPORTANT`); the rest get freshness only.
* **``expected_empty`` is driven by the catalog's ``skip_if_none`` /
  ``skip_if_empty`` flags.** Those endpoints *write nothing* when empty, so an
  unsupported collection has zero captures — ``expected_empty`` makes freshness
  pass on zero captures (instead of ERRORing) and content-health pass on empties.
  Empty collections still get a content check so a *genuine error* envelope warns.

Reference/unpartitioned collections have no event timeline → ``event_date_source``
is ``"none"`` (completeness skipped). Garmin captures with ``dedupe=False``, so
every scheduled run writes — sidecar freshness is reliable here.

* **Excluded endpoints get no checks.** A collection turned off via
  ``FETCH_EXCLUDE`` / ``FETCH_SELECTION`` never writes again, so a freshness check
  on it would inevitably age past tolerance and page. :func:`_config_for` gates on
  :meth:`settings.is_selected`, the same predicate that drives capture — if we
  don't pull it, we don't check it.
"""

from __future__ import annotations

from grecohome_core.checks import (
    CollectionCheckConfig,
    build_bronze_checks_job,
    build_bronze_checks_schedule,
    build_collection_checks,
)
from grecohome_garmin import catalog
from grecohome_garmin.catalog import KIND_DAILY, KIND_PER_DEVICE_RANGE, KIND_RANGE
from grecohome_garmin.config import settings
from grecohome_garmin.dagster.assets import ASSET_BY_COLLECTION

# Kinds whose data is date-oriented → daily-partitioned assets (mirrors assets.py).
_DAILY_KINDS = frozenset({KIND_DAILY, KIND_RANGE, KIND_PER_DEVICE_RANGE})

# The analytically-important daily time-series that earn the full check suite.
# Everything else gets freshness only (plus content if it's expected-empty).
IMPORTANT: frozenset[str] = frozenset(
    {
        "sleep",
        "stress",
        "hrv",
        "respiration",
        "spo2",
        "resting_heart_rate",
        "heart_rates",
        "steps_intraday",
        "intensity_minutes",
        "training_status",
        "training_readiness",
        "max_metrics",
        "body_battery",
        "user_summary",
    }
)

# Collections that are legitimately (near-)always empty on THIS hardware, derived
# from a content sweep of the live bronze tree (not just the catalog's skip flags,
# which miss several: these write an empty payload daily rather than returning None,
# so skip_if_* never fires). ~85%+ of recent payloads are empty for each. Marking
# them expected_empty stops content-health from crying wolf on the normal case
# while still warning on a genuine error envelope.
EMPIRICALLY_EMPTY: frozenset[str] = frozenset(
    {
        "training_readiness",
        "body_battery_events",
        "running_tolerance",
        "goals",
        "max_metrics",
        "hrv",
        "activities",  # intermittent: ~82% of days have no activity
    }
)


def _config_for(ep: catalog.Endpoint) -> CollectionCheckConfig | None:
    """Derive a check config for one catalog endpoint, or None if it has no asset
    or the endpoint is excluded from capture."""
    asset = ASSET_BY_COLLECTION.get(ep.collection)
    if asset is None:
        return None

    # Don't check what we don't capture. An endpoint excluded via FETCH_EXCLUDE /
    # FETCH_SELECTION never writes again, so its freshness check is guaranteed to
    # eventually age past tolerance and page (ERROR) for data we *deliberately*
    # stopped pulling -- exactly how a quiet female-health/pregnancy exclusion
    # turns into a nightly critical alert. Gate on the same predicate as capture.
    if not settings.is_selected(ep.name):
        return None

    is_daily = ep.kind in _DAILY_KINDS
    # Catalog skip flags catch the endpoints that return None (zero captures);
    # EMPIRICALLY_EMPTY catches the ones that write an empty payload daily.
    expected_empty = ep.skip_if_none or ep.skip_if_empty or ep.collection in EMPIRICALLY_EMPTY
    important = ep.collection in IMPORTANT

    if important:
        enabled = frozenset({"freshness", "completeness", "schema", "content"})
    elif expected_empty:
        enabled = frozenset({"freshness", "content"})
    else:
        enabled = frozenset({"freshness"})

    return CollectionCheckConfig(
        source="garmin",
        collection=ep.collection,
        asset_key=asset.key,
        reader="json",
        unnest_records=False,  # flat DTO-style payloads
        event_date_source="partition" if is_daily else "none",
        cadence_hours=26.0,  # daily schedule -> a capture roughly every 24h
        grace_hours=6.0,
        cadence_days=14 if expected_empty else 2,  # widen for sparse/empty collections
        expected_empty=expected_empty,
        enabled_checks=enabled,
    )


GARMIN_CHECK_CONFIGS: list[CollectionCheckConfig] = [
    cfg for ep in catalog.CATALOG if (cfg := _config_for(ep)) is not None
]

garmin_checks = build_collection_checks(
    GARMIN_CHECK_CONFIGS,
    bronze_root=settings.bronze_root,
    monitor_dir=settings.bronze_monitor_dir,
)

# Checks-only job + hourly schedule (off the garmin_api pool).
garmin_checks_job = build_bronze_checks_job(garmin_checks, name="garmin_bronze_checks_job")
garmin_checks_schedule = build_bronze_checks_schedule(
    garmin_checks_job, name="garmin_bronze_checks_hourly"
)
