"""Catalog-driven pull helpers.

Decomposes garmincapture's monolithic runner into per-endpoint helpers the Dagster
assets call (one asset per collection). Each helper fetches via the live
``garminconnect`` client and captures through the core bronze writer (append-only).

Skip-identical (``dedupe``) is decided **per call site**, never from
``Endpoint.kind`` alone (the per-activity fan-out uses synthetic ``static``
endpoints). It is on only where one request writes a ``(collection, dt)`` folder
per run *and* the endpoint returns a value every run (no ``skip_if_*`` flag) --
see :func:`skip_identical` and ``docs/intent/garmin/capture/capture-design.md``.

Failure model (differs from the original always-on runner): legitimate "no data"
returns (``None``, or empty when ``skip_if_empty``) write nothing and return
quietly; **any real exception propagates** so the owning Dagster asset fails
visibly and is retried — rather than being swallowed. Auth/429 naturally
propagate too.
"""

import time
from collections import Counter
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


# @spec GARMIN-CAPTURE-004, GARMIN-CAPTURE-005, GARMIN-CAPTURE-CHECKS-002
def skip_identical(ep: Endpoint) -> bool:
    """Whether a single-request capture of ``ep`` may skip an identical payload.

    Skip-flagged endpoints write nothing on no-data days, so their re-captures of
    older days are what keep the freshness check fed -- they keep every capture.
    Single-request paths compare with the folder's newest capture; fan-out paths
    (per activity / device / goal status) pass ``dedupe_scope="request"`` so each
    request is compared only with its own earlier captures.
    """
    return not (ep.skip_if_empty or ep.skip_if_none)


class GarminPuller:
    """Fetch + capture catalog endpoints against a live ``Garmin`` client."""

    def __init__(self, client: Garmin, settings: GarminSettings, *, bronze_root: str | None = None):
        self.client = client
        self.settings = settings
        self.bronze_root = bronze_root or settings.bronze_root
        self.outcomes: Counter[str] = Counter()

    def outcome_counts(self) -> dict[str, int]:
        """Per-run counts of capture outcomes, for materialization metadata."""
        return {
            k: self.outcomes[k] for k in ("written", "skipped_identical", "failed")
        }

    def _record(self, result) -> None:
        self.outcomes[str(result.outcome)] += 1

    def _pace(self) -> None:
        if self.settings.rate_limit_seconds > 0:
            time.sleep(self.settings.rate_limit_seconds)

    # @spec GARMIN-CAPTURE-001, GARMIN-CAPTURE-002, GARMIN-CAPTURE-003
    def _fetch_and_capture(
        self,
        ep: Endpoint,
        fn,
        request_params: dict,
        *,
        dt: str | None,
        dedupe: bool,
        dedupe_scope: str = "folder",
    ) -> Any:
        """Call ``fn``; capture unless it's a no-data return. Exceptions propagate.

        ``None`` is never captured. An empty list/dict is captured *unless* the
        endpoint is flagged ``skip_if_empty`` (an empty 200 is otherwise a faithful
        "asked, no data" record). Returns the parsed value whether or not the
        capture was written (a skipped activities list still drives the fan-out).
        """
        parsed = fn()
        if parsed is None:
            return None
        if ep.skip_if_empty and not parsed:
            return parsed
        result = capture.capture_json(
            ep.collection,
            parsed,
            request_url=ep.method,
            request_params=request_params,
            bronze_root=self.bronze_root,
            processor_version=self.settings.processor_version,
            dt=dt,
            screen_secrets=ep.screen_secrets,
            dedupe=dedupe,
            dedupe_scope=dedupe_scope,
        )
        self._record(result)
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
        dedupe = skip_identical(ep)  # one request per folder for these kinds
        if ep.kind == KIND_DAILY:
            return self._fetch_and_capture(
                ep, lambda: method(cdate), {"cdate": cdate}, dt=dt or cdate, dedupe=dedupe
            )
        if ep.kind == KIND_RANGE:
            return self._fetch_and_capture(
                ep, lambda: method(start, end), {"start": start, "end": end}, dt=dt,
                dedupe=dedupe,
            )
        if ep.kind == KIND_WEEKLY:
            weeks = self.settings.weekly_weeks
            return self._fetch_and_capture(
                ep, lambda: method(end, weeks), {"end": end, "weeks": weeks}, dt=dt,
                dedupe=dedupe,
            )
        if ep.kind == KIND_STATIC:
            return self._fetch_and_capture(ep, method, {}, dt=dt, dedupe=dedupe)
        if ep.kind == KIND_STATIC_GOALS:
            # @spec GARMIN-CAPTURE-008
            # One request per status into one folder: compare per request.
            for status in GOAL_STATUSES:
                self._fetch_and_capture(
                    ep, lambda s=status: method(status=s), {"status": status}, dt=dt,
                    dedupe=dedupe, dedupe_scope="request",
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
            dedupe=skip_identical(ep),
        )

    # @spec GARMIN-CAPTURE-007
    def pull_per_device(
        self,
        ep: Endpoint,
        *,
        start: str | None = None,
        end: str | None = None,
        dt: str | None = None,
    ) -> None:
        """Per-device endpoints, looped over the devices discovered from the API.

        One request per device into one folder: compare per request (a skip-flagged
        endpoint keeps every capture).
        """
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
                    dedupe=skip_identical(ep),
                    dedupe_scope="request",
                )
            elif ep.kind == KIND_PER_DEVICE_RANGE:
                self._fetch_and_capture(
                    ep,
                    lambda d=device_id: getattr(self.client, ep.method)(str(d), start, end),
                    {"device_id": device_id, "start": start, "end": end},
                    dt=dt,
                    dedupe=skip_identical(ep),
                    dedupe_scope="request",
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
            dedupe=skip_identical(activities_ep),
        )
        for act in activities or []:
            aid = act.get("activityId") if isinstance(act, dict) else None
            if aid is None:
                continue
            self._fan_out_activity(aid, dt=dt)
            self._pace()

    # @spec GARMIN-CAPTURE-006
    def _fan_out_activity(self, aid, *, dt: str | None) -> None:
        """Per-activity captures: one request per activity into each folder, compared
        per request, so identical answers for different activities are kept while an
        unchanged re-capture of the same activity is skipped."""
        for name, method in catalog.PER_ACTIVITY:
            if not self.settings.is_selected(name):
                continue
            ep = Endpoint(name, method, KIND_STATIC, name)
            self._fetch_and_capture(
                ep,
                lambda m=method, a=aid: getattr(self.client, m)(a),
                {"activity_id": aid},
                dt=dt,
                dedupe=True,
                dedupe_scope="request",
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
        result = capture.capture_raw(
            collection,
            raw,
            ext=ext,
            content_type=content_type,
            request_url="download_activity",
            request_params={"activity_id": aid, "format": fmt_name},
            bronze_root=self.bronze_root,
            processor_version=self.settings.processor_version,
            dt=dt,
            dedupe=True,
            dedupe_scope="request",
        )
        self._record(result)
