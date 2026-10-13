"""Lingo subject configuration.

Extends the shared ``BaseSubjectSettings`` with the Google Drive knobs. Auth is a
**service account** (mounted key JSON) — no interactive OAuth, no token refresh.
Fields map case-insensitively to ``GDRIVE_*`` env vars (Ansible-injected).
"""

from grecohome_core.config import BaseSubjectSettings, init_settings


class LingoSettings(BaseSubjectSettings):
    """Settings for the Lingo data subject (extends the shared base)."""

    # Both required (no default): an empty folder id would make the sensor skip
    # silently forever, so a missing setting stops the code location at startup.
    # @spec LINGO-AUTH-002
    # Google Drive folder to watch (by stable folder id, not name).
    gdrive_folder_id: str

    # Path to the mounted service-account key JSON (shared read-only on the
    # Drive folder). Never under BRONZE_ROOT.
    gdrive_service_account_path: str

    # Minimum interval (minutes) for the Drive-watching sensor.
    # @spec LINGO-SENSOR-004
    gdrive_poll_interval_minutes: int = 5


settings: LingoSettings = init_settings(LingoSettings)  # type: ignore[assignment]
