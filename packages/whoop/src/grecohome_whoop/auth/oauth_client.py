"""OAuth 2.0 client for Whoop API authentication.

Implements the OAuth 2.0 authorization code flow with PKCE for secure
authentication with the Whoop API.
"""

import base64
import secrets
from typing import Any
from urllib.parse import urlencode

import httpx
from authlib.oauth2.rfc7636 import create_s256_code_challenge
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from grecohome_core.logging_config import get_logger
from grecohome_whoop.config import settings

logger = get_logger(__name__)

# Transient token-endpoint failures worth a brief retry. 4xx (esp. 400
# invalid_grant / 401) are terminal: the refresh token is consumed or revoked and
# only re-auth recovers it, so retrying just hammers the endpoint with a dead grant.
_RETRYABLE_TOKEN_STATUS = frozenset({429, 500, 502, 503, 504})
_TOKEN_EXP_WAIT = wait_exponential(multiplier=1, min=1, max=8)

# Whoop's token endpoint is routinely slow (observed refreshes take 1-4s) and its
# rotation is *non-atomic*: if a refresh POST reaches Whoop, the refresh token is
# consumed and rotated (R -> R') even if we never receive the response. httpx's
# default 5s timeout turned exactly that into a dead grant (2026-07-20): the read
# timed out after Whoop had already rotated, so we kept -- and replayed -- the
# consumed R. Give the rotation ample time to complete and be persisted; keep the
# connect phase short so a genuinely-down network still fails fast.
_TOKEN_TIMEOUT = httpx.Timeout(30.0, connect=5.0)

# Transport errors safe to retry on the *refresh* endpoint: only those where the
# request provably never reached Whoop, so no rotation could have happened. A
# ReadTimeout / WriteError / RemoteProtocolError means the POST may already have
# been received and the refresh token consumed -- replaying it double-spends the
# single-use token and revokes the grant, so those must NOT be retried.
_CONNECT_ONLY_TRANSPORT_ERRORS = (
    httpx.ConnectError,
    httpx.ConnectTimeout,
    httpx.PoolTimeout,
)


def _parse_retry_after(value: str | None) -> float | None:
    """Parse a Retry-After header (delta-seconds form). HTTP-date form -> None."""
    if not value:
        return None
    try:
        return max(0.0, float(value))
    except (TypeError, ValueError):
        return None


def _is_retryable_token_error(exc: BaseException) -> bool:
    """True for transient token-endpoint failures safe to retry — never 4xx.

    Retries a 5xx/429 response or a *connect-phase* transport error (the request
    never landed). Deliberately does NOT retry a read/write timeout or mid-flight
    network error: on the refresh endpoint the POST may already have consumed the
    single-use refresh token server-side, so replaying it would revoke the grant.
    """
    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code in _RETRYABLE_TOKEN_STATUS
    return isinstance(exc, _CONNECT_ONLY_TRANSPORT_ERRORS)


def _token_retry_wait(retry_state) -> float:
    """Honor a server Retry-After on transient failures, else exponential backoff."""
    exc = retry_state.outcome.exception() if retry_state.outcome else None
    if isinstance(exc, httpx.HTTPStatusError):
        retry_after = _parse_retry_after(exc.response.headers.get("Retry-After"))
        if retry_after is not None:
            return min(retry_after, 30.0)
    return _TOKEN_EXP_WAIT(retry_state)


class WhoopOAuthClient:
    """OAuth 2.0 client for Whoop API authentication.

    Implements authorization-code flow with PKCE. Handles authorization-URL
    generation and token exchange/refresh.
    """

    def __init__(self) -> None:
        """Initialize OAuth client from settings."""
        self.client_id = settings.whoop_client_id
        self.client_secret = settings.whoop_client_secret
        self.redirect_uri = settings.whoop_redirect_uri
        self.auth_url = settings.whoop_auth_url
        self.token_url = settings.whoop_token_url

        # Required scopes for data access.
        self.scopes = [
            "read:sleep",
            "read:workout",
            "read:recovery",
            "read:cycles",
            "read:profile",  # /user/profile/basic (the snapshots asset)
            "read:body_measurement",  # Height, weight, max heart rate
            "offline",  # For refresh tokens
        ]

        logger.info(
            "OAuth client initialized",
            client_id=self.client_id[:8] + "...",
            redirect_uri=self.redirect_uri,
            scopes=self.scopes,
        )

    def generate_pkce_pair(self) -> tuple[str, str]:
        """Generate a PKCE (RFC 7636) code verifier and challenge."""
        code_verifier = (
            base64.urlsafe_b64encode(secrets.token_bytes(32)).decode("utf-8").rstrip("=")
        )
        code_challenge = create_s256_code_challenge(code_verifier)
        logger.debug(
            "Generated PKCE pair",
            verifier_length=len(code_verifier),
            challenge=code_challenge[:10] + "...",
        )
        return code_verifier, code_challenge

    def get_authorization_url(
        self,
        state: str | None = None,
    ) -> tuple[str, str, str]:
        """Generate the authorization URL for user consent.

        Args:
            state: Optional CSRF token; generated when omitted.

        Returns:
            Tuple of (authorization_url, state, code_verifier).
        """
        code_verifier, code_challenge = self.generate_pkce_pair()
        if state is None:
            state = secrets.token_urlsafe(32)

        params = {
            "client_id": self.client_id,
            "redirect_uri": self.redirect_uri,
            "response_type": "code",
            "scope": " ".join(self.scopes),
            "state": state,
            "code_challenge": code_challenge,
            "code_challenge_method": "S256",
        }
        auth_url = f"{self.auth_url}?{urlencode(params)}"
        logger.info("Generated authorization URL", state=state[:10] + "...", scopes=self.scopes)
        return auth_url, state, code_verifier

    async def exchange_code_for_token(
        self,
        code: str,
        code_verifier: str,
    ) -> dict[str, Any]:
        """Exchange an authorization code for access/refresh tokens.

        Raises:
            httpx.HTTPStatusError: If the token exchange fails.
        """
        data = {
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": self.redirect_uri,
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "code_verifier": code_verifier,
        }
        logger.info("Exchanging authorization code for token", code=code[:10] + "...")
        try:
            async with httpx.AsyncClient(timeout=_TOKEN_TIMEOUT) as client:
                response = await client.post(
                    self.token_url,
                    data=data,
                    headers={"Content-Type": "application/x-www-form-urlencoded"},
                )
                response.raise_for_status()
                token_data = response.json()
                logger.info(
                    "Successfully exchanged code for token",
                    token_type=token_data.get("token_type"),
                    expires_in=token_data.get("expires_in"),
                    scopes=token_data.get("scope"),
                )
                return token_data
        except httpx.HTTPStatusError as e:
            # Log status only -- the body / exc_info carries secrets.
            logger.error("Token exchange failed", status_code=e.response.status_code)
            raise
        except Exception as e:
            logger.error("Unexpected error during token exchange", error=str(e), exc_info=True)
            raise

    @retry(
        stop=stop_after_attempt(3),
        wait=_token_retry_wait,
        retry=retry_if_exception(_is_retryable_token_error),
        reraise=True,
    )
    async def refresh_access_token(
        self,
        refresh_token: str,
    ) -> dict[str, Any]:
        """Refresh an expired access token.

        Retried up to 3 times with backoff on a 5xx/429 response or a connect-phase
        transport error (the request never reached Whoop). A read/write timeout or
        mid-flight network error is NOT retried: Whoop may already have consumed and
        rotated the single-use refresh token, so replaying it would revoke the grant.
        A 4xx (400 invalid_grant / 401) is terminal and re-raised immediately — the
        refresh token is consumed or revoked and only re-auth
        (``python -m grecohome_whoop.oauth_setup``) recovers it.

        Raises:
            httpx.HTTPStatusError: If the refresh fails (token expired/revoked).
            httpx.TransportError: On a network/timeout failure (not retried when the
                request may already have consumed the token).
        """
        data = {
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }
        logger.info("Refreshing access token")
        try:
            async with httpx.AsyncClient(timeout=_TOKEN_TIMEOUT) as client:
                response = await client.post(
                    self.token_url,
                    data=data,
                    headers={"Content-Type": "application/x-www-form-urlencoded"},
                )
                response.raise_for_status()
                token_data = response.json()
                logger.info(
                    "Successfully refreshed access token",
                    token_type=token_data.get("token_type"),
                    expires_in=token_data.get("expires_in"),
                )
                return token_data
        except httpx.HTTPStatusError as e:
            # Log status only -- the body / exc_info carries secrets.
            status = e.response.status_code
            if status in (400, 401):
                # Terminal: the refresh token is dead. Emit a distinct, greppable
                # event so alerting can page for re-auth immediately, instead of
                # waiting out the token-health check's grace window.
                logger.error("whoop_token_invalid_grant", status_code=status)
            else:
                logger.error("Token refresh failed", status_code=status)
            raise
        except Exception as e:
            logger.error("Unexpected error during token refresh", error=str(e), exc_info=True)
            raise
