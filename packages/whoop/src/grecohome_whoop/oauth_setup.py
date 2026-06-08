#!/usr/bin/env python3
"""Interactive OAuth setup for the Whoop data subject.

Guides you through the OAuth 2.0 authorization-code (PKCE) flow and writes the
resulting tokens to the JSON file at ``WHOOP_TOKEN_PATH``. Single-user system:
there is no user record, just the one token file.

Usage:
    # Normal mode (opens a browser, runs a local callback server)
    python -m grecohome_whoop.oauth_setup

    # Headless mode (authorize on another device, paste the callback URL)
    python -m grecohome_whoop.oauth_setup --headless
"""

import argparse
import asyncio
import sys
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs, urlparse

from grecohome_core.logging_config import get_logger
from grecohome_whoop.auth.oauth_client import WhoopOAuthClient
from grecohome_whoop.auth.token_manager import TokenManager
from grecohome_whoop.config import settings

logger = get_logger(__name__)


class OAuthCallbackHandler(BaseHTTPRequestHandler):
    """HTTP handler that captures the OAuth callback's code/state."""

    auth_code: str | None = None
    state: str | None = None
    error: str | None = None

    def do_GET(self):
        params = parse_qs(urlparse(self.path).query)
        OAuthCallbackHandler.auth_code = params.get("code", [None])[0]
        OAuthCallbackHandler.state = params.get("state", [None])[0]
        OAuthCallbackHandler.error = params.get("error", [None])[0]

        self.send_response(200 if OAuthCallbackHandler.auth_code else 400)
        self.send_header("Content-type", "text/html")
        self.end_headers()
        if OAuthCallbackHandler.auth_code:
            msg = "Authorization successful</h1><p>You can close this window."
        else:
            msg = "Authorization failed</h1><p>No code received."
        self.wfile.write(f"<html><body><h1>{msg}</p></body></html>".encode())

    def log_message(self, *args):
        pass


async def _wait_for_callback(server: HTTPServer, timeout: int = 300):
    """Block until the callback fires (or timeout), returning (code, state, error)."""
    loop = asyncio.get_event_loop()
    try:
        await asyncio.wait_for(loop.run_in_executor(None, server.handle_request), timeout=timeout)
    except TimeoutError:
        logger.error("Timeout waiting for OAuth callback")
        return None, None, "timeout"
    return (
        OAuthCallbackHandler.auth_code,
        OAuthCallbackHandler.state,
        OAuthCallbackHandler.error,
    )


def _scopes_from(token_data: dict) -> list[str]:
    scope = token_data.get("scope") or ""
    return scope.split() if scope else []


def _save(token_manager: TokenManager, token_data: dict) -> None:
    token_manager.save_token(
        access_token=token_data["access_token"],
        refresh_token=token_data["refresh_token"],
        expires_in=token_data["expires_in"],
        token_type=token_data.get("token_type", "Bearer"),
        scopes=_scopes_from(token_data),
    )


async def setup_headless() -> bool:
    """Authorize without a local browser: print URL, read the pasted callback."""
    print("\n" + "=" * 70)
    print("Whoop OAuth Setup (Headless Mode)")
    print("=" * 70 + "\n")

    oauth_client = WhoopOAuthClient()
    token_manager = TokenManager(oauth_client)
    auth_url, state, code_verifier = oauth_client.get_authorization_url()

    print("Open this URL in a browser on any device:\n")
    print(f"  {auth_url}\n")
    print("After authorizing you'll be redirected to a callback URL like:")
    print(f"  {oauth_client.redirect_uri}?code=XXXX&state=XXXX\n")

    callback_url = input("Paste the full callback URL here: ").strip()
    params = parse_qs(urlparse(callback_url).query)
    auth_code = params.get("code", [None])[0]
    received_state = params.get("state", [None])[0]
    if params.get("error", [None])[0]:
        print(f"\nAuthorization failed: {params['error'][0]}")
        return False
    if not auth_code:
        print("\nNo authorization code found in the URL.")
        return False
    if received_state != state:
        print("\nState mismatch (possible CSRF). Start over.")
        return False

    token_data = await oauth_client.exchange_code_for_token(auth_code, code_verifier)
    _save(token_manager, token_data)
    print(f"\n✓ Tokens written to {settings.whoop_token_path}\n")
    return True


async def setup_browser() -> bool:
    """Authorize via a local browser + callback server."""
    print("\n" + "=" * 60)
    print("Whoop OAuth Setup")
    print("=" * 60 + "\n")

    oauth_client = WhoopOAuthClient()
    token_manager = TokenManager(oauth_client)
    auth_url, state, code_verifier = oauth_client.get_authorization_url()

    port = urlparse(oauth_client.redirect_uri).port or 8000
    try:
        server = HTTPServer(("localhost", port), OAuthCallbackHandler)
    except OSError as e:
        print(f"Could not start callback server on port {port}: {e}")
        return False

    print(f"Opening browser; callback server on port {port}.")
    print(f"If the browser doesn't open, visit:\n\n  {auth_url}\n")
    webbrowser.open(auth_url)
    print("Waiting for authorization (times out in 5 minutes)...\n")

    auth_code, received_state, error = await _wait_for_callback(server)
    server.server_close()

    if error:
        print(f"\nAuthorization failed: {error}")
        return False
    if not auth_code:
        print("\nNo authorization code received.")
        return False
    if received_state != state:
        print("\nState mismatch (possible CSRF). Start over.")
        return False

    token_data = await oauth_client.exchange_code_for_token(auth_code, code_verifier)
    _save(token_manager, token_data)
    print(f"\n✓ Tokens written to {settings.whoop_token_path}\n")
    return True


async def main() -> None:
    parser = argparse.ArgumentParser(description="Set up Whoop OAuth authorization")
    parser.add_argument(
        "--headless",
        action="store_true",
        help="Headless mode (no local browser; paste the callback URL)",
    )
    args = parser.parse_args()

    ok = await (setup_headless() if args.headless else setup_browser())
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\nSetup cancelled.\n")
        sys.exit(1)
