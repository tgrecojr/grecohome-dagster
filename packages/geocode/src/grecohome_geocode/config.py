"""Geocode (Photon reverse-geocoder) subject configuration.

Extends the shared ``BaseSubjectSettings`` with the Photon knobs. The source is a
**self-hosted** Photon HTTP service (no auth, no secret), so there is nothing to mount
beyond the bronze root. Fields map case-insensitively to env vars (Ansible-injected).

There is deliberately **no state dir**: the "already-cached" set is derived from the
geocode bronze sidecars themselves (each records its ``lat_e4``/``lon_e4`` cell), so the
cache is its own durable idempotency ledger — nothing extra to keep outside bronze.
"""

from grecohome_core.config import BaseSubjectSettings, init_settings


class GeocodeSettings(BaseSubjectSettings):
    """Settings for the geocode data subject (extends the shared base)."""

    # Base URL of the self-hosted Photon service, e.g. ``http://photon:2322``. The
    # subject appends ``/reverse`` itself. Per the photon-docker docs, do NOT include
    # the ``/api`` path here. Required (no secret — Photon is auth-less on the LAN).
    photon_base_url: str

    # Per-request timeout (seconds) for the Photon call.
    photon_timeout: float = 30.0

    # Language for Photon's ``lang`` param (localizes place names).
    photon_language: str = "en"

    # Photon ``/reverse`` search radius (km). Small so we anchor on the nearest object,
    # but non-zero so the full FeatureCollection of nearby candidates is returned and
    # cached raw — future silver logic can pick among candidates without re-querying.
    photon_radius_km: float = 0.05  # ~50 m

    # Trailing window (UTC days, incl. today) of location bronze the discovery step
    # scans each run for observed cells. Keep comfortably larger than worst-case geocode
    # downtime. To backfill the cache over ALL history once, widen this for a single run
    # (the cache is permanent, so the wide scan need only happen once per resolution).
    geocode_scan_days: int = 7

    # Safety valve: cap how many new cells one materialization looks up (a first,
    # wide-window backfill can surface thousands of new cells). The remainder are picked
    # up on the next run; the cap is logged so a truncated run is never silent.
    geocode_max_lookups_per_run: int = 2000

    # Trailing bronze partitions the checks inspect (keeps checks fast as bronze grows).
    geocode_recent_partitions: int = 14


settings: GeocodeSettings = init_settings(GeocodeSettings)  # type: ignore[assignment]
