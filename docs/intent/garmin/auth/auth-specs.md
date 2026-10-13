# Garmin authentication — EARS specs

Design: [auth-design.md](auth-design.md). Prefix `GARMIN-AUTH`. Facets: none (login and the
token store), `HEALTH` (the token-store health check).

## Login and the token store

- [x] **GARMIN-AUTH-001**: Before a Garmin run's login, the system shall log `garmin_tokenstore_unusable` at ERROR, with the token-store path and a reason, when `GARMINTOKENS` holds inline token JSON (`inline_json`), the library refuses its path, as it does for a symlink anywhere in it (`path_rejected`), the token file's directory exists but isn't a writable, searchable directory (`not_writable`), the directory is missing and its nearest existing parent isn't writable (`parent_not_writable`), or no directory above the token file other than `/` is a mount point (`off_volume`); the system shall then proceed with the login.
- [x] **GARMIN-AUTH-002**: After a Garmin run's login, if the token file is missing, or it was last modified before that login started and its parsed contents differ from the client's in-memory session (`dumps()`), then the system shall log `garmin_tokenstore_not_saved` at ERROR with the token-store path, never the contents.
- [x] **GARMIN-AUTH-003**: The system shall run the Garmin token-store checks (GARMIN-AUTH-001 and GARMIN-AUTH-002) once per run, outside the login retry, so a retried login doesn't repeat them.
- [x] **GARMIN-AUTH-004**: If a Garmin login fails with `GarminConnectAuthenticationError` or `GarminConnectConnectionError`, then the system shall retry the whole login with a fresh client, up to 3 attempts in total.
- [x] **GARMIN-AUTH-005**: When the Garmin bootstrap (`bootstrap.py`) finishes its login, if the session wasn't saved to the token store (the GARMIN-AUTH-002 condition), then the bootstrap shall exit non-zero instead of reporting success.

## Token-store health check

- [x] **GARMIN-AUTH-HEALTH-001**: The `garmin_tokenstore_health` asset check (on `garmin_bronze_sleep`, run hourly in `garmin_bronze_checks_job`) shall fail at ERROR severity, log `garmin_tokenstore_unhealthy` and post an alert when the token store is unusable for any GARMIN-AUTH-001 reason, the token file is missing or unreadable, or the file isn't JSON containing `di_token`, `di_refresh_token` and `di_client_id`.
- [x] **GARMIN-AUTH-HEALTH-002**: The `garmin_tokenstore_health` check shall report only the token-store path and the failure reason, never any part of the token file's contents.
