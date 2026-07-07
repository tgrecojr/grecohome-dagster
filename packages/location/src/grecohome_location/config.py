"""Location subject configuration.

Extends the shared ``BaseSubjectSettings`` with the promoter knobs. This subject
makes **no source-API calls** and holds **no secret**: it reads the relay's staging
directory (mounted read-only) and promotes each file into bronze. Fields map
case-insensitively to env vars (Ansible-injected).

Two paths must stay outside ``BRONZE_ROOT``: the relay staging dir (the relay is its
sole writer/cleaner) and the promoted-set state dir (bronze stays immutable raw
capture). A ``model_validator`` fails fast if ``location_state_dir`` lands under
``bronze_root``.
"""

from __future__ import annotations

import os

from pydantic import model_validator

from grecohome_core.config import BaseSubjectSettings, init_settings


class LocationSettings(BaseSubjectSettings):
    """Settings for the location data subject (extends the shared base)."""

    # The relay's data dir (host /opt/docker/locationrelay/data). Mounted READ-ONLY;
    # the promoter never writes or deletes under it. Required.
    relay_capture_dir: str

    # Writable dir for the per-stream promoted-set (the capture-once ledger). MUST be
    # outside BRONZE_ROOT (enforced below). Required.
    location_state_dir: str

    # Trailing staging window (in days, incl. today) the promoter scans each run. Keep
    # comfortably larger than worst-case promoter downtime, and smaller than the
    # relay's retention (default 14d) so nothing is pruned before promotion.
    location_promote_window_days: int = 3

    # Promote-lag ERROR threshold: an un-promoted staging file older than this many
    # hours means the promoter is not keeping up (data risks relay-retention pruning).
    location_promote_lag_hours: float = 6.0

    # Receipt-freshness tolerances (hours since the newest received POST in bronze).
    # Location is event-driven, so gaps are usually legitimate: WARN wide, ERROR only
    # past a very long gap.
    location_freshness_warn_hours: float = 24.0
    location_freshness_error_hours: float = 168.0  # 7 days

    # Trailing bronze partitions the checks inspect (keeps checks fast as bronze grows).
    location_recent_partitions: int = 14

    @model_validator(mode="after")
    def _state_dir_outside_bronze(self) -> LocationSettings:
        """Refuse a promoted-set state dir that resolves inside ``bronze_root``."""
        if not self.location_state_dir or not self.bronze_root:
            return self
        state = os.path.abspath(self.location_state_dir)
        bronze = os.path.abspath(self.bronze_root)
        try:
            within = os.path.commonpath([state, bronze]) == bronze
        except ValueError:  # different drives, etc.
            within = False
        if within:
            raise ValueError(
                f"LOCATION_STATE_DIR ({state}) must be OUTSIDE BRONZE_ROOT ({bronze}); "
                "the promoted-set is state, and bronze must stay immutable raw capture."
            )
        return self


settings: LocationSettings = init_settings(LocationSettings)  # type: ignore[assignment]
