"""Catalog-driven pull helpers.

Decomposes garmincapture's monolithic runner into per-endpoint helpers the Dagster
assets call (one asset per collection). Each helper fetches via the live
``garminconnect`` client and captures through the core bronze writer (append-only,
``dedupe=False``).

Failure model (differs from the original always-on runner): legitimate "no data"
returns (``None``, or empty when ``skip_if_empty``) write nothing and return
quietly; **any real exception propagates** so the owning Dagster asset fails
visibly and is retried — rather than being swallowed. Auth/429 naturally
propagate too.
"""

import time
from typing import Any

from garminconnect import Garmin

from grecohome_core.logging_config import get_logger
from grecohome_garmin import capture, catalog
from grecohome_garmin.catalog import (
    GOAL_STATUSES,
    KIND_DAILY,
    KIND_PER_DEVICE,
    KIND_PER_DEVICE_RANGE,
    KIND_RANGE,
    KIND_STATIC,
    KIND_STATIC_GOALS,
    KIND_WEEKLY,
    Endpoint,
)
from grecohome_garmin.config import GarminSettings

log = get_logger(__name__)


class GarminPuller:
    """Fetch + capture catalog endpoints against a live ``Garmin`` client."""

    def __init__(self, client: Garmin, settings: GarminSettings, *, bronze_root: str | None = None):
        self.client = client
        self.settings = settings
        self.bronze_root = bronze_root or settings.bronze_root

    def _pace(self) -> None:
        if self.settings.rate_limit_seconds > 0:
            time.sleep(self.settings.rate_limit_seconds)

    def _fetch_and_capture(self, ep: Endpoint, fn, request_params: dict, *, dt: str | None) -> Any:
        """Call ``fn``; capture unless it's a no-data return. Exceptions propagate.

        ``None`` is never captured. An empty list/dict is captured *unless* the
        endpoint is flagged ``skip_if_empty`` (an empty 200 is otherwise a faithful
        "asked, no data" record).
        """
        parsed = fn()
        if parsed is None:
            return None
        if ep.skip_if_empty and not parsed:
            return parsed
        capture.capture_json(
            ep.collection,
            parsed,
            request_url=ep.method,
            request_params=request_params,
            bronze_root=self.bronze_root,
            processor_version=self.settings.processor_version,
            dt=dt,
            screen_secrets=ep.screen_secrets,
        )
        return parsed

    def pull_endpoint(
        self,
        ep: Endpoint,
        *,
        cdate: str | None = None,
        start: str | None = None,
        end: str | None = None,
        dt: str | None = None,
    ) -> Any:
        """Fetch + capture a single date/static endpoint, dispatching on ``ep.kind``.

        For ``KIND_DAILY`` the capture ``dt`` defaults to ``cdate`` (event date).
        Per-profile / per-device / activities have dedicated methods.
        """
        method = getattr(self.client, ep.method)
        if ep.kind == KIND_DAILY:
            return self._fetch_and_capture(
                ep, lambda: method(cdate), {"cdate": cdate}, dt=dt or cdate
            )
        if ep.kind == KIND_RANGE:
            return self._fetch_and_capture(
                ep, lambda: method(start, end), {"start": start, "end": end}, dt=dt
            )
        if ep.kind == KIND_WEEKLY:
            weeks = self.settings.weekly_weeks
            return self._fetch_and_capture(
                ep, lambda: method(end, weeks), {"end": end, "weeks": weeks}, dt=dt
            )
        if ep.kind == KIND_STATIC:
            return self._fetch_and_capture(ep, method, {}, dt=dt)
        if ep.kind == KIND_STATIC_GOALS:
            for status in GOAL_STATUSES:
                self._fetch_and_capture(
                    ep, lambda s=status: method(status=s), {"status": status}, dt=dt
                )
                self._pace()
            return None
        raise ValueError(f"pull_endpoint cannot handle kind {ep.kind!r}")

    def pull_per_profile(self, ep: Endpoint, *, dt: str | None = None) -> None:
        """Endpoints keyed by the user's profile number (discovered from settings)."""
        profile = self.client.get_userprofile_settings()
        profile_number = profile.get("id") if isinstance(profile, dict) else None
        if profile_number is None:
            log.debug("per_profile_skipped", collection=ep.collection, reason="no profile id")
            return
        self._fetch_and_capture(
            ep,
            lambda: getattr(self.client, ep.method)(profile_number),
            {"user_profile_number": profile_number},
            dt=dt,
        )

    def pull_per_device(
        self,
        ep: Endpoint,
        *,
        start: str | None = None,
        end: str | None = None,
        dt: str | None = None,
    ) -> None:
        """Per-device endpoints, looped over the devices discovered from the API."""
        devices = self.client.get_devices()
        if not isinstance(devices, list):
            return
        for dev in devices:
            device_id = dev.get("deviceId") if isinstance(dev, dict) else None
            if device_id is None:
                continue
            if ep.kind == KIND_PER_DEVICE:
                self._fetch_and_capture(
                    ep,
                    lambda d=device_id: getattr(self.client, ep.method)(d),
                    {"device_id": device_id},
                    dt=dt,
                )
            elif ep.kind == KIND_PER_DEVICE_RANGE:
                self._fetch_and_capture(
                    ep,
                    lambda d=device_id: getattr(self.client, ep.method)(str(d), start, end),
                    {"device_id": device_id, "start": start, "end": end},
                    dt=dt,
                )
            self._pace()

    def pull_activities(self, start: str, end: str, *, dt: str | None = None) -> None:
        """Capture the activities list for the window, then fan out per activity."""
        activities_ep = catalog.get("activities")
        activities = self._fetch_and_capture(
            activities_ep,
            lambda: self.client.get_activities_by_date(start, end),
            {"start": start, "end": end},
            dt=dt,
        )
        for act in activities or []:
            aid = act.get("activityId") if isinstance(act, dict) else None
            if aid is None:
                continue
            self._fan_out_activity(aid, dt=dt)
            self._pace()

    def _fan_out_activity(self, aid, *, dt: str | None) -> None:
        for name, method in catalog.PER_ACTIVITY:
            if not self.settings.is_selected(name):
                continue
            ep = Endpoint(name, method, KIND_STATIC, name)
            self._fetch_and_capture(
                ep,
                lambda m=method, a=aid: getattr(self.client, m)(a),
                {"activity_id": aid},
                dt=dt,
            )
            self._pace()

        downloads = list(catalog.PER_ACTIVITY_DOWNLOAD)
        if self.settings.capture_alt_formats:
            downloads += list(catalog.PER_ACTIVITY_ALT_DOWNLOAD)
        for collection, fmt_name in downloads:
            if not self.settings.is_selected(collection):
                continue
            self._download_activity(aid, collection, fmt_name, dt=dt)
            self._pace()

    def _download_activity(self, aid, collection: str, fmt_name: str, *, dt: str | None) -> None:
        """Capture a binary activity download at the ``raw`` grade (true bytes)."""
        fmt = getattr(Garmin.ActivityDownloadFormat, fmt_name)
        raw = self.client.download_activity(str(aid), fmt)
        if not raw:
            return
        ext, content_type = capture.DOWNLOAD_EXT.get(fmt_name, ("bin", "application/octet-stream"))
        capture.capture_raw(
            collection,
            raw,
            ext=ext,
            content_type=content_type,
            request_url="download_activity",
            request_params={"activity_id": aid, "format": fmt_name},
            bronze_root=self.bronze_root,
            processor_version=self.settings.processor_version,
            dt=dt,
        )
