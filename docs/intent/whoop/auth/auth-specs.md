# Whoop token lifecycle — EARS specs

Design: [auth-design.md](auth-design.md). Prefix `WHOOP-AUTH`. Facets: none (token
lifecycle), `LOG` (credential-safe logging), `HEALTH` (the token-health check).

## OAuth token lifecycle

- [x] **WHOOP-AUTH-001**: While the stored Whoop access token has more than 5 minutes until its `expires_at`, the system shall return it without contacting the Whoop token endpoint.
- [x] **WHOOP-AUTH-002**: When the stored Whoop access token is within 5 minutes of its `expires_at`, the system shall refresh it before returning an access token.
- [x] **WHOOP-AUTH-003**: While a Whoop token refresh holds the host-wide file lock at `<WHOOP_TOKEN_PATH>.lock`, the system shall make every other refreshing caller, in any process on the host, wait for that lock before reading or refreshing the token.
- [x] **WHOOP-AUTH-004**: When a caller acquires a Whoop refresh lock (the in-process lock or the host-wide file lock) and the re-read stored token is no longer within 5 minutes of expiry, the system shall return that token without refreshing.
- [x] **WHOOP-AUTH-005**: The system shall send every Whoop refresh request with `scope=offline`.
- [x] **WHOOP-AUTH-006**: The system shall give every Whoop token-endpoint request (refresh and authorization-code exchange) a 30-second overall timeout and a 5-second connect timeout.
- [x] **WHOOP-AUTH-007**: If a Whoop token refresh fails with a connect-phase transport error (`ConnectError`, `ConnectTimeout` or `PoolTimeout`), then the system shall retry the refresh, up to 3 attempts in total.
- [x] **WHOOP-AUTH-008**: If a Whoop token refresh receives any HTTP error response (4xx or 5xx), or fails with any transport error other than a connect-phase error (such as a read or write timeout), then the system shall raise without retrying the refresh.
- [x] **WHOOP-AUTH-009**: If obtaining a Whoop access token fails for any reason while the Whoop API client prepares a data request, then the system shall raise a non-retryable `WhoopAuthError`, so that the data-request retry never repeats a token refresh.
- [x] **WHOOP-AUTH-010**: If no Whoop token file exists when the Whoop API client prepares a data request, then the system shall raise a non-retryable `WhoopAuthError`.
- [x] **WHOOP-AUTH-011**: When a Whoop refresh returns a response with `access_token`, `expires_in` and a rotated `refresh_token`, the system shall atomically replace the stored token with the new access token, the rotated refresh token and an `expires_at` of now plus `expires_in`.
- [x] **WHOOP-AUTH-012**: If a Whoop refresh returns a response with `access_token` and `expires_in` but no rotated `refresh_token`, then the system shall keep the current refresh token and log a `whoop_token_no_rotation` warning.
- [x] **WHOOP-AUTH-013**: If a Whoop refresh returns a response that lacks `access_token` or `expires_in` but carries a rotated `refresh_token`, then the system shall persist that rotated refresh token with an `expires_at` in the past, and then raise.
- [x] **WHOOP-AUTH-014**: If a Whoop refresh returns a response that lacks `access_token` or `expires_in` and carries no rotated `refresh_token`, then the system shall raise without modifying the stored token file.
- [x] **WHOOP-AUTH-015**: If a Whoop token refresh receives HTTP 400 or 401, then the system shall log the event `whoop_token_invalid_grant` at ERROR level with the status code.
- [x] **WHOOP-AUTH-016**: When a Whoop re-auth (`oauth_setup`, through `TokenManager.save_token`) writes a fresh token, the system shall hold the host-wide refresh file lock at `<WHOOP_TOKEN_PATH>.lock` for the write.

## Credential-safe logging

- [x] **WHOOP-AUTH-LOG-001**: If an unexpected failure (anything other than an HTTP error status) occurs in the Whoop authorization-code exchange, token refresh or API request path, then the system shall log only the message and the exception's type name, with no traceback and no exception text.
- [x] **WHOOP-AUTH-LOG-002**: If the Whoop authorization-code exchange, token refresh or API request path re-raises an unexpected failure as a Whoop exception, then the system shall put only the original exception's type name in the new exception's message.
- [x] **WHOOP-AUTH-LOG-003**: The Whoop subject shall never log any part, including a prefix, of an access token, refresh token, client secret, authorization code or PKCE verifier.
- [x] **WHOOP-AUTH-LOG-004**: The Whoop subject shall never log the body of a Whoop token-endpoint response or API error response.
- [x] **WHOOP-AUTH-LOG-005**: Where nothing has configured structlog in a process running Whoop code, the shared logger (`grecohome_core.logging_config.get_logger`) shall render exception tracebacks without frame-local variables, and otherwise keep structlog's default output format, with no ANSI color codes when output is not a terminal.

## Token health check

- [x] **WHOOP-AUTH-HEALTH-001**: If the stored Whoop token's `expires_at` is more than 90 minutes in the past when the `whoop_token_health` check runs, then the system shall fail the check at ERROR severity and log `whoop_token_unhealthy`.
- [x] **WHOOP-AUTH-HEALTH-002**: If the Whoop token file is missing or unreadable (invalid JSON, or no `expires_at`) when the `whoop_token_health` check runs, then the system shall fail the check at ERROR severity rather than raising.
