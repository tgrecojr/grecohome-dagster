"""Token management for Whoop OAuth tokens.

Persists OAuth token state to a plaintext JSON file (a mounted, writable path) via
:class:`grecohome_core.tokens.file_store.TokenFileStore`, and refreshes the access
token when it nears expiry. Whoop rotates the refresh token on every refresh, so a
refresh rewrites the whole file atomically.

Single-user system: the file holds exactly one user's tokens. ``user_id`` is kept
on the public surface only so the Whoop client can call ``get_valid_token(user_id)``
unchanged; its value is otherwise vestigial.
"""

import asyncio
from datetime import UTC, datetime, timedelta
from typing import NamedTuple

from grecohome_core.logging_config import get_logger
from grecohome_core.tokens.file_store import TokenFileStore
from grecohome_whoop.auth.oauth_client import WhoopOAuthClient
from grecohome_whoop.config import settings

logger = get_logger(__name__)

# Single-user system (see CLAUDE.md): the token file holds one user's tokens.
USER_ID = 1


class _TokenState(NamedTuple):
    """Snapshot of stored token state, used to decide on refresh."""

    access_token: str
    refresh_token: str
    expires_at: datetime
    token_type: str
    scopes: list[str]


def _parse_expires_at(value: str) -> datetime:
    """Parse a stored ISO ``expires_at``, assuming UTC if it carries no offset."""
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt


class TokenManager:
    """Manage the Whoop OAuth token lifecycle over a plaintext-JSON file store."""

    # Per-user refresh locks, shared across all TokenManager instances in the
    # process. Whoop rotates the refresh token on every refresh, so two
    # concurrent refreshes would race: the second presents an already-consumed
    # refresh token and can revoke the grant. Serializing per user (the event
    # loop is single-threaded, so get-or-create is atomic between awaits) ensures
    # one in-flight refresh per user. Across separate run processes the Dagster
    # `whoop_api` concurrency pool serializes API work, so only one run holds the
    # token at a time.
    _refresh_locks: dict[int, asyncio.Lock] = {}

    @classmethod
    def _get_refresh_lock(cls, user_id: int) -> asyncio.Lock:
        """Return the process-wide refresh lock for a user, creating it once."""
        lock = cls._refresh_locks.get(user_id)
        if lock is None:
            lock = asyncio.Lock()
            cls._refresh_locks[user_id] = lock
        return lock

    def __init__(
        self,
        oauth_client: WhoopOAuthClient | None = None,
        refresh_threshold_minutes: int = 5,
        store: TokenFileStore | None = None,
    ) -> None:
        """Initialize the token manager.

        Args:
            oauth_client: OAuth client for refreshes (created if None).
            refresh_threshold_minutes: Refresh when within this many minutes of expiry.
            store: Token file store (defaults to ``TokenFileStore(settings.whoop_token_path)``).
        """
        self.oauth_client = oauth_client or WhoopOAuthClient()
        self.refresh_threshold = timedelta(minutes=refresh_threshold_minutes)
        self.store = store or TokenFileStore(settings.whoop_token_path)
        logger.info(
            "Token manager initialized",
            refresh_threshold_minutes=refresh_threshold_minutes,
        )

    def save_token(
        self,
        access_token: str,
        refresh_token: str,
        expires_in: int,
        token_type: str = "Bearer",
        scopes: list[str] | None = None,
    ) -> None:
        """Write a fresh token to the file store (used by the OAuth init flow)."""
        expires_at = datetime.now(UTC) + timedelta(seconds=expires_in)
        self.store.write_atomic(
            {
                "access_token": access_token,
                "refresh_token": refresh_token,
                "token_type": token_type,
                "expires_at": expires_at.isoformat(),
                "scopes": scopes or [],
            }
        )
        logger.info("Saved token", expires_at=expires_at.isoformat())

    async def get_valid_token(self, user_id: int = USER_ID) -> str | None:
        """Return a valid access token, refreshing if near expiry.

        Refreshes are serialized per user so concurrent callers never each spend
        the rotating refresh token.

        Returns:
            A valid access token, or ``None`` if no token is stored.

        Raises:
            Exception: If a needed refresh fails.
        """
        # Fast path: read current state without taking the refresh lock.
        state = self._read_token_state()
        if state is None:
            logger.warning("No token found", user_id=user_id)
            return None

        now = datetime.now(UTC)
        if state.expires_at - now > self.refresh_threshold:
            return state.access_token

        # Near expiry: serialize refresh per user. The lock holder refreshes;
        # everyone else falls through to the freshly-stored token.
        async with self._get_refresh_lock(user_id):
            state = self._read_token_state()
            if state is None:
                return None
            now = datetime.now(UTC)
            if state.expires_at - now > self.refresh_threshold:
                logger.info("Token refreshed by a concurrent caller; reusing", user_id=user_id)
                return state.access_token

            logger.info(
                "Token near expiration, refreshing",
                user_id=user_id,
                time_until_expiry_seconds=(state.expires_at - now).total_seconds(),
            )
            try:
                token_data = await self.oauth_client.refresh_access_token(state.refresh_token)
            except Exception as e:
                # No exc_info: the exception's locals (the access/refresh tokens)
                # would otherwise be rendered into logs. str(e) is the safe
                # status-only message; the oauth client logs the specific cause.
                logger.error("Failed to refresh token", user_id=user_id, error=str(e))
                raise

            # Validate before mutating stored state, so a malformed 200 can't
            # leave the file half-updated.
            if not token_data.get("access_token") or token_data.get("expires_in") is None:
                raise ValueError("Token refresh response missing access_token/expires_in")

            return self._store_refreshed_token(token_data, state)

    def _read_token_state(self) -> _TokenState | None:
        """Read the stored token into a snapshot (no refresh), or None if absent."""
        data = self.store.read()
        if not data:
            return None
        return _TokenState(
            access_token=data["access_token"],
            refresh_token=data["refresh_token"],
            expires_at=_parse_expires_at(data["expires_at"]),
            token_type=data.get("token_type", "Bearer"),
            scopes=data.get("scopes", []),
        )

    def _store_refreshed_token(self, token_data: dict, current: _TokenState) -> str:
        """Persist a refreshed token atomically and return the new access token."""
        new_access_token = token_data["access_token"]
        expires_at = datetime.now(UTC) + timedelta(seconds=token_data["expires_in"])
        rotated_refresh_token = token_data.get("refresh_token")
        if not rotated_refresh_token:
            # Whoop rotates the refresh token on every refresh, so a 200 without
            # one is anomalous. Keep the current token (don't drop the grant), but
            # surface it loudly — silently re-persisting a possibly-consumed token
            # is exactly how a refresh failure turns into a silent multi-hour outage.
            logger.warning("whoop_token_no_rotation")
        self.store.write_atomic(
            {
                "access_token": new_access_token,
                # Whoop rotates the refresh token; fall back to the current one if absent.
                "refresh_token": rotated_refresh_token or current.refresh_token,
                "token_type": token_data.get("token_type", current.token_type),
                "expires_at": expires_at.isoformat(),
                "scopes": current.scopes,
            }
        )
        logger.info("Token refreshed successfully", new_expires_at=expires_at.isoformat())
        return new_access_token

    async def is_token_valid(self, user_id: int = USER_ID) -> bool:
        """Return True if a stored, unexpired token exists."""
        state = self._read_token_state()
        if state is None:
            return False
        return state.expires_at > datetime.now(UTC)
