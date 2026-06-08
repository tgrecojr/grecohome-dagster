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

from grecohome_core.logging_config import get_logger
from grecohome_whoop.config import settings

logger = get_logger(__name__)


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
            async with httpx.AsyncClient() as client:
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

    async def refresh_access_token(
        self,
        refresh_token: str,
    ) -> dict[str, Any]:
        """Refresh an expired access token.

        Raises:
            httpx.HTTPStatusError: If the refresh fails (token expired/revoked).
        """
        data = {
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }
        logger.info("Refreshing access token")
        try:
            async with httpx.AsyncClient() as client:
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
            logger.error("Token refresh failed", status_code=e.response.status_code)
            raise
        except Exception as e:
            logger.error("Unexpected error during token refresh", error=str(e), exc_info=True)
            raise
