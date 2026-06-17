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
from garminconnect.exceptions import (
    GarminConnectAuthenticationError,
    GarminConnectConnectionError,
)
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from grecohome_core.logging_config import get_logger
from grecohome_garmin.config import GarminSettings

log = get_logger(__name__)

# Transient Garmin SSO/login failures worth retrying. The library's own
# ``retry_attempts`` covers individual HTTP calls, but NOT the SSO ticket->cookie
# exchange, which intermittently raises "JWT_WEB cookie not set after ticket
# consumption" (and assorted connection blips). That failure surfaces in Dagster
# *resource init*, before any op runs, so an op-level RetryPolicy can't catch it
# -- retrying the whole login here (fresh client each attempt) clears the blip
# without operator intervention. Mirrors the tenacity idiom in whoop_client.py.
_LOGIN_RETRYABLE = (GarminConnectAuthenticationError, GarminConnectConnectionError)
_LOGIN_MAX_ATTEMPTS = 3


def _log_login_retry(retry_state) -> None:
    """Structured log line emitted before each login retry sleep."""
    exc = retry_state.outcome.exception() if retry_state.outcome else None
    log.warning(
        "garmin_login_retry",
        attempt=retry_state.attempt_number,
        max_attempts=_LOGIN_MAX_ATTEMPTS,
        error=str(exc) if exc else None,
    )


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


@retry(
    stop=stop_after_attempt(_LOGIN_MAX_ATTEMPTS),
    wait=wait_exponential(multiplier=1, min=2, max=15),
    retry=retry_if_exception_type(_LOGIN_RETRYABLE),
    before_sleep=_log_login_retry,
    reraise=True,
)
def login(settings: GarminSettings, prompt_mfa=_default_prompt_mfa) -> Garmin:
    """Authenticate and return a live ``Garmin`` client.

    Resumes from the token store when possible; falls back to a full credentialed
    login (which re-writes the token store) otherwise. The library handles all
    retry/refresh internally for its own HTTP calls; on top of that we retry the
    *whole* login on transient SSO/connection failures (a fresh client each
    attempt) so a flaky ticket->cookie exchange doesn't fail the Dagster run.
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
