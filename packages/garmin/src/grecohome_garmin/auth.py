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
import json
import os
import sys
import time
from pathlib import Path

from garminconnect import Garmin
from garminconnect.client import token_file_path
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

# The session fields a usable token file must carry (garminconnect DI tokens).
_SESSION_FIELDS = ("di_token", "di_refresh_token", "di_client_id")

# Mount-point test, indirected so tests can say which directories are mounts.
_is_mount = os.path.ismount


def _is_inline_json(tokenstore: str) -> bool:
    """garminconnect also accepts token JSON in place of a path; it can't be saved back."""
    return tokenstore.strip().startswith("{")


def _display(tokenstore: str) -> str:
    """The token-store value safe to log: a path, never inline token JSON."""
    return "<inline token JSON>" if _is_inline_json(tokenstore) else tokenstore


def token_file(tokenstore: str) -> Path:
    """The token file garminconnect loads and saves for ``tokenstore``.

    Uses the library's own resolution (a directory or non-``.json`` path means
    ``<path>/garmin_tokens.json``). Raises ``ValueError`` for a path the library
    refuses, such as one with a symlink anywhere in it.
    """
    return token_file_path(tokenstore)


# @spec GARMIN-AUTH-001
def tokenstore_problem(tokenstore: str) -> str | None:
    """Why the token store can't keep a session across runs, or ``None`` if it can.

    The library fails silently on all of these (a debug log on load, a swallowed
    error on save), so every run would quietly do a full password login.
    """
    if _is_inline_json(tokenstore):
        return "inline_json"
    try:
        directory = token_file(tokenstore).parent
    except ValueError:
        return "path_rejected"
    writable = os.W_OK | os.X_OK
    if directory.exists():
        if not (directory.is_dir() and os.access(directory, writable)):
            return "not_writable"
    else:
        parent = next(p for p in directory.parents if p.exists())
        if not os.access(parent, writable):
            return "parent_not_writable"
    if not any(_is_mount(p) for p in (directory, *directory.parents) if p != Path("/")):
        return "off_volume"
    return None


# @spec GARMIN-AUTH-002
def session_saved(tokenstore: str, session: str, since: float) -> bool:
    """Whether the token file holds a current session after a login.

    True if the file holds exactly ``session`` (the client's in-memory session),
    or was rewritten at or after ``since`` (another run saved its own fresh
    session -- the store is live). A read-only store that still holds an old
    file fails both. Contents are compared in memory only, never logged.
    """
    if _is_inline_json(tokenstore):
        return False
    try:
        path = token_file(tokenstore)
        if path.stat().st_mtime >= since:
            return True
        return json.loads(path.read_text()) == json.loads(session)
    except (OSError, ValueError):
        return False


# @spec GARMIN-AUTH-HEALTH-001, GARMIN-AUTH-HEALTH-002
def tokenstore_health(tokenstore: str) -> tuple[bool, dict]:
    """Pure token-store verdict for the health check: ``(passed, metadata)``.

    Metadata carries only the (safe) path and a reason -- never file contents.
    """
    metadata: dict = {"tokenstore": _display(tokenstore)}
    problem = tokenstore_problem(tokenstore)
    if problem:
        return False, {**metadata, "reason": problem}
    try:
        data = json.loads(token_file(tokenstore).read_text())
    except FileNotFoundError:
        return False, {**metadata, "reason": "missing"}
    except OSError:
        return False, {**metadata, "reason": "unreadable"}
    except ValueError:
        return False, {**metadata, "reason": "not_json"}
    if not isinstance(data, dict) or not all(data.get(f) for f in _SESSION_FIELDS):
        return False, {**metadata, "reason": "missing_session_fields"}
    return True, metadata


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


# @spec GARMIN-AUTH-003
def login(settings: GarminSettings, prompt_mfa=_default_prompt_mfa) -> Garmin:
    """Authenticate and return a live ``Garmin`` client, checking the token store.

    The token-store checks run once per run, around (not inside) the login retry:
    an unusable store is logged before logging in, and an unsaved session after.
    Neither stops the login -- a broken store makes logins expensive (a full
    password login every run), which shouldn't by itself stop capture.
    """
    tokenstore = settings.garmintokens
    problem = tokenstore_problem(tokenstore)
    if problem:
        log.error("garmin_tokenstore_unusable", tokenstore=_display(tokenstore), reason=problem)
    started = time.time()
    client = _login_with_retry(settings, prompt_mfa)
    if not session_saved(tokenstore, client.client.dumps(), since=started):
        log.error("garmin_tokenstore_not_saved", tokenstore=_display(tokenstore))
    return client


# @spec GARMIN-AUTH-004
@retry(
    stop=stop_after_attempt(_LOGIN_MAX_ATTEMPTS),
    wait=wait_exponential(multiplier=1, min=2, max=15),
    retry=retry_if_exception_type(_LOGIN_RETRYABLE),
    before_sleep=_log_login_retry,
    reraise=True,
)
def _login_with_retry(settings: GarminSettings, prompt_mfa=_default_prompt_mfa) -> Garmin:
    """Authenticate and return a live ``Garmin`` client (retried as a whole).

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
        tokenstore=_display(settings.garmintokens),
        is_cn=settings.garminconnect_is_cn,
    )
    client.login(tokenstore=settings.garmintokens)
    log.info("garmin_login_ok")
    return client
