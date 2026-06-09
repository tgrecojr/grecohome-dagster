"""Garmin subject configuration.

Extends the shared ``BaseSubjectSettings`` with the Garmin-specific knobs. Field
names map case-insensitively to garmincapture's env var names (``GARMINTOKENS``,
``GARMINCONNECT_*``, ``LOOKBACK_DAYS``, ``FETCH_SELECTION``/``FETCH_EXCLUDE``, …)
so the existing host secret/convention carries over. The always-on poll loop is
gone (Dagster schedules it), so ``POLL_INTERVAL_SECONDS`` is dropped.
"""

from grecohome_core.config import BaseSubjectSettings, init_settings


class GarminSettings(BaseSubjectSettings):
    """Settings for the Garmin data subject (extends the shared base)."""

    # --- Garmin auth (delegated to the garminconnect library) ---
    # Token store path (mounted, writable, never under BRONZE_ROOT). The library
    # writes and self-heals it; first run is an interactive MFA bootstrap.
    garmintokens: str = "/tokens"
    garminconnect_email: str = ""
    garminconnect_base64_password: str = ""
    garminconnect_is_cn: bool = False

    # --- Pull window / pacing ---
    # Trailing days a schedule re-captures (covers Garmin's restatement lag and
    # missed runs). Backfill (dagster backfill) overrides this.
    lookback_days: int = 7
    rate_limit_seconds: float = 2.0
    # Trailing weeks requested from the weekly-aggregate endpoints.
    weekly_weeks: int = 4

    # --- Selection / toggles ---
    # Allowlist of catalog names (empty => everything not excluded).
    fetch_selection: str = ""
    # Denylist of catalog names; WINS over selection. Default empty = capture the
    # full surface (incl. female-health endpoints). Set e.g.
    # FETCH_EXCLUDE=hrv,training_readiness to skip known-empty streams.
    fetch_exclude: str = ""
    capture_alt_formats: bool = False

    # --- Provenance ---
    processor_version: str = "dev"

    @property
    def fetch_selection_set(self) -> set[str]:
        """Parsed ``FETCH_SELECTION`` as a set of catalog names (empty => all)."""
        return {name.strip() for name in self.fetch_selection.split(",") if name.strip()}

    @property
    def fetch_exclude_set(self) -> set[str]:
        """Parsed ``FETCH_EXCLUDE`` as a set of catalog names (denylist)."""
        return {name.strip() for name in self.fetch_exclude.split(",") if name.strip()}

    def is_selected(self, name: str) -> bool:
        """Whether catalog endpoint ``name`` runs this invocation.

        Exclusion always wins over selection; an empty selection means
        "everything not excluded".
        """
        if name in self.fetch_exclude_set:
            return False
        selection = self.fetch_selection_set
        return not selection or name in selection


settings: GarminSettings = init_settings(GarminSettings)  # type: ignore[assignment]
