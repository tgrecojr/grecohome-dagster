"""Authentication -- fully delegated to ``garminconnect``.

We never reimplement Garmin's auth (Cloudflare challenges, token-audience
rejections, MFA, CN domains, token refresh). The library handles that; we consume
it and track the latest version.

The token store lives on a mounted volume (``GARMINTOKENS``). The library writes
and self-heals it. **No credential, token, or auth response ever touches bronze**
-- login happens here and returns a live client; the token store is never under
``BRONZE_ROOT``.

First run is interactive for MFA (the bootstrap wires ``prompt_mfa`` to stdin);
subsequent runs resume silently from the persisted token store.
"""

import base64
import sys

from garminconnect import Garmin

from grecohome_core.logging_config import get_logger
from grecohome_garmin.config import GarminSettings

log = get_logger(__name__)


def _decode_password(b64_password: str) -> str:
    """Decode the base64-encoded password (mirrors garmin-grafana's convention)."""
    if not b64_password:
        return ""
    return base64.b64decode(b64_password).decode("utf-8")


def _default_prompt_mfa() -> str:
    """Interactive MFA prompt used on first-run token bootstrap only."""
    sys.stderr.write("Garmin MFA code required: ")
    sys.stderr.flush()
    return input().strip()


def login(settings: GarminSettings, prompt_mfa=_default_prompt_mfa) -> Garmin:
    """Authenticate and return a live ``Garmin`` client.

    Resumes from the token store when possible; falls back to a full credentialed
    login (which re-writes the token store) otherwise. The library handles all
    retry/refresh internally.
    """
    email = settings.garminconnect_email or None
    password = _decode_password(settings.garminconnect_base64_password) or None

    client = Garmin(
        email=email,
        password=password,
        is_cn=settings.garminconnect_is_cn,
        prompt_mfa=prompt_mfa,
        retry_attempts=3,
        verify_login=True,
    )

    log.info(
        "garmin_login_start",
        tokenstore=settings.garmintokens,
        is_cn=settings.garminconnect_is_cn,
    )
    client.login(tokenstore=settings.garmintokens)
    log.info("garmin_login_ok")
    return client
