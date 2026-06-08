"""Whoop subject configuration."""

from grecohome_core.config import BaseSubjectSettings, init_settings


class WhoopSettings(BaseSubjectSettings):
    """Settings for the Whoop data subject (extends the shared base)."""

    # Whoop OAuth
    whoop_client_id: str
    whoop_client_secret: str
    whoop_redirect_uri: str = "http://localhost:8000/callback"
    whoop_api_base_url: str = "https://api.prod.whoop.com"
    whoop_auth_url: str = "https://api.prod.whoop.com/oauth/oauth2/auth"
    whoop_token_url: str = "https://api.prod.whoop.com/oauth/oauth2/token"

    # Path to the OAuth token JSON file (mounted, writable). Required.
    whoop_token_path: str

    # API rate cap (in-process, within-run guard).
    max_requests_per_minute: int = 60

    # Trailing reconcile overlap (days). The hourly schedule re-captures this
    # many days + 1 partition so Whoop's retroactive rescores/deletes are
    # eventually re-captured (bronze just appends + content-hash dedups).
    reconcile_window_days: int = 7


settings: WhoopSettings = init_settings(WhoopSettings)  # type: ignore[assignment]
