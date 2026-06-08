# ADR 0003: OAuth tokens in a plaintext-JSON file

## Status
Accepted.

## Context
`whoopster` stored OAuth tokens in a Postgres `oauth_tokens` table, encrypted at rest with
Fernet (`TOKEN_ENCRYPTION_KEY`). Postgres is being removed (see [[0001-bronze-only]]), so token
storage needs a new home. Whoop **rotates the refresh token on every refresh**, so whatever
stores it must be writable and crash-safe.

## Decision
- **Store tokens as a plaintext JSON file** at a mounted, writable path (`WHOOP_TOKEN_PATH`),
  managed by the secrets manager / Ansible. Drop Fernet and `TOKEN_ENCRYPTION_KEY` entirely —
  encryption-at-rest is provided by the mounted volume / secrets manager, not the app.
- **Atomic writes.** `TokenFileStore.write_atomic` writes to a temp file in the same directory,
  `fsync`s, then `os.replace`s — so a crash mid-write never corrupts the file or loses the
  rotated refresh token.
- **Rotation-safe refresh.** `TokenManager` keeps the per-user async refresh lock and the
  "fall back to the current refresh token if the response omits one" behavior. Across separate
  run processes, the `whoop_api` pool (limit 1) ensures only one run touches the token at a time,
  removing the rotation race.
- **Single user.** The file holds exactly one user's tokens (`USER_ID = 1`); `user_id` survives
  on the API only so the Whoop client call site is unchanged.

## Consequences
- The token directory must be a writable mount, separate from `BRONZE_ROOT`.
- One-time setup is `python -m grecohome_whoop.oauth_setup [--headless]`, which writes the file.
- No encryption key to manage; protection is at the volume/secrets-manager layer.

## Related
[[0001-bronze-only]], [[0002-dagster-pins]].
