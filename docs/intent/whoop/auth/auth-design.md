---
parent: whoop
prefix: WHOOP-AUTH
---

# Whoop token lifecycle

## Context

Whoop's single-use, rotating refresh token (see the parent [Whoop design](../whoop-design.md))
makes the token the one piece of state whose loss stops all capture. This component keeps a
valid access token available to the API client while **never presenting a refresh token
twice**, and keeps every credential out of the logs.

## OAuth token lifecycle

### Storage

The token is plaintext JSON at `WHOOP_TOKEN_PATH`: a mounted, writable path outside
`BRONZE_ROOT`. It holds `access_token`, `refresh_token`, `token_type`, `expires_at` (ISO
UTC) and `scopes`. Every write replaces the whole file atomically (temp file plus rename),
so a reader never sees a half-written token, and a crash mid-write can't lose a rotated
refresh token. The file holds one user's tokens (single-user system, `USER_ID = 1`). There's no
app-level encryption: the mounted volume and the secrets manager protect it.

### When to refresh

`TokenManager.get_valid_token` returns the stored access token while it has more than
**5 minutes** left. Inside that threshold it refreshes. The schedule cadence matches the
token lifetime, so in practice each hourly tick performs exactly one refresh.

### Serializing the refresh

Several processes need the token at the same moment: the Dagster step subprocesses, plus the
bronze and snapshots runs that start on the same minute. Two of them refreshing at once
would both present `R`, and the second would revoke the grant. The refresh critical section
is therefore guarded twice:

1. An `asyncio.Lock` serializes callers inside one process.
2. A host-wide `fcntl` file lock at `<WHOOP_TOKEN_PATH>.lock` serializes across processes.
   The kernel releases it if the holder dies.

After acquiring each lock, the caller **re-reads the token file**. If another caller has
already refreshed it (it is no longer within the threshold), the waiter reuses that token
instead of refreshing again.

### The refresh request

- A `POST` to the token URL with `grant_type=refresh_token`, `client_id`, `client_secret`,
  and **`scope=offline`**. Whoop's refresh flow uses `scope=offline` to keep issuing a
  rotated refresh token.
- Timeouts: **30 s** overall and **5 s** to connect. Whoop's token endpoint routinely takes
  1–4 s. A short read timeout fires after Whoop has already rotated `R`, which loses `R'`.

### Retry rule

A refresh is retried (up to 3 attempts, exponential backoff of 1–8 s) **only on a
connect-phase transport error**: `ConnectError`, `ConnectTimeout` or `PoolTimeout`. In those
cases the request provably never reached Whoop, so `R` was never presented.

That 3-attempt budget is the **only** retry a refresh ever gets. The API client fetches its
bearer token inside its own request retry, which retries timeouts and network errors on
*data* requests. So any error from obtaining the token reaches the API client as a
**non-retryable auth error** (`WhoopAuthError`). Otherwise a refresh that timed out after
Whoop had already rotated `R` would be re-run by the data-request retry, replaying `R`.

Anything else is raised without retry:
- any HTTP response (4xx or 5xx),
- a read or write timeout,
- any other mid-flight error.

In each of those cases Whoop may have received `R`. A transient 5xx fails one run; the next
tick refreshes with `R` intact, if Whoop did not consume it.

### Handling the response

- The response must contain `access_token` and `expires_in`. Otherwise the refresh raises.
  If such a malformed 200 still carries a rotated `refresh_token`, that `R'` is persisted
  first, with `expires_at` set in the past, so the next caller refreshes with `R'` instead of
  replaying the consumed `R`. A malformed 200 with no `R'` leaves the file untouched.
- The rotated `refresh_token` replaces the stored one, and `expires_at` is set to now plus
  `expires_in`.
- If a 200 carries no rotated refresh token, the current one is kept and a
  `whoop_token_no_rotation` warning is logged. This shouldn't happen, and making it loud
  means a silent re-use of a consumed token can't hide.

### Missing or unreadable token file

- **No token file:** `get_valid_token` returns `None`, and the API client raises a
  non-retryable `WhoopAuthError` ("no valid access token").
- **Unreadable file** (invalid JSON, or a required key missing): reading it raises. The run
  fails without retrying.
- **The `whoop_token_health` check** reports either case as a failed check, not as an error
  in the check itself.

### Writing a fresh token

A re-auth (`oauth_setup`) writes the new token while holding the same host-wide file lock as
a refresh. Without the lock, a refresh already in flight on the old grant could finish later
and overwrite the fresh token.

### Failure signals and recovery

| Signal | Meaning |
|---|---|
| `whoop_token_invalid_grant` (ERROR, `status_code` 400/401) | The refresh token is dead. Re-auth is needed now. |
| `Token refresh failed` (ERROR, other status) | Whoop returned an error. Not retried; the next tick tries again. |
| `whoop_token_health` asset check (ERROR), logging `whoop_token_unhealthy` (WARNING) and the shared `asset_check_failed` (ERROR) | `expires_at` is more than 90 minutes in the past, or the token file is missing or unreadable: refreshes have stopped succeeding, or stopped happening. The check reads only the token file (no Whoop API call) and runs hourly at `:37` in `whoop_bronze_checks_job`. |

The only recovery from a revoked grant is a re-auth: `scripts/whoop-reauth.sh` on the host
runs `grecohome_whoop.oauth_setup --headless` inside a throwaway container built from the
live image, environment and volumes. `docs/WHOOP_TOKEN_RUNBOOK.md` has the procedure.

## Credential-safe logging

Per the HLD decision *Logs never carry credentials*, these Whoop paths hold credentials in
local variables:

- the authorization-code exchange (holds `client_secret`, the authorization code and the PKCE
  verifier),
- the token refresh (holds `client_secret` and the refresh token),
- the API request (holds the bearer access token).

On those paths:

- **Expected failures** (an HTTP error status) log the message and `status_code`.
- **Unexpected failures** log the message and the **exception type name only**. They log no
  traceback (`exc_info`) and no exception text: exception text can embed a header value, such
  as an HTTP library rejecting a malformed `Authorization` header. Exceptions they re-raise
  carry the type name, not the original text.
- **Response bodies are never logged.** A token response body *is* the credential.
- **No part of a token, client secret, authorization code or PKCE verifier is logged**, not
  even a prefix. Identifiers that are not secrets may be logged in short prefixes for
  correlation: the client id, the OAuth `state` and the PKCE challenge.

The interactive re-auth (`oauth_setup`) prints the authorization URL to its own terminal for
the user to open. That URL carries only the client id, `state` and challenge.

**How tracebacks render in each kind of process:**

- **Run and step worker processes** (where assets execute) use the shared core logger. Its
  safe defaults render tracebacks **without local variables**. This is the backstop for any
  traceback logged on another path.
- **The code server and gRPC processes** are configured by Dagster itself, which formats
  tracebacks through stdlib logging, without locals. Deployment must not pass Dagster's
  `--log-format rich`, whose renderer shows locals.

## Decisions & Alternatives

| Decision | Chosen | Alternatives Considered | Rationale |
|---|---|---|---|
| Token storage | Plaintext JSON at a mounted, writable `WHOOP_TOKEN_PATH`, rewritten atomically | A Fernet-encrypted Postgres table (`TOKEN_ENCRYPTION_KEY`) | There's no transactional database (HLD), and the refresh token rotates every refresh, so storage must be writable and crash-safe. Encryption at rest belongs to the volume and secrets manager, which also leaves no app key to manage. |
| Refresh retry policy | Retry connect-phase transport errors only | Retry 5xx/429 with backoff; retry all transport errors | Retrying anything that may have reached Whoop replays `R` and revokes the grant. A 5xx retry and a read-timeout retry each killed a grant. The cost is that a transient 5xx fails one run. |
| Refresh timeout | 30 s overall, 5 s connect | httpx default (5 s) | The endpoint routinely takes 1–4 s. A 5 s read timeout fired after rotation and lost the new token. |
| Cross-process serialization | `fcntl` file lock next to the token file, plus re-read after lock | Rely on the host `whoop_api` pool limit; a database lock | The pool limit is host configuration and was not reliably 1. A file lock works on the single host that mounts the token directory, and needs no extra service (the HLD rules out a transactional database). |
| `scope=offline` on refresh | Sent | Omit it, scope only at authorization | Matches Whoop's documented refresh flow. Omitting it risks a 200 without a rotated refresh token. |
| Missing rotation in a 200 | Keep the current refresh token and warn | Fail the refresh | Failing would drop a possibly valid grant. The warning makes the anomaly visible. |
| Refresh inside the data-request retry | A token error becomes a non-retryable `WhoopAuthError` | Fetch the token once per run, outside the retry; give data retries their own predicate | Wrapping the error at the single point the client obtains a token keeps every call site correct, and it leaves the refresh's own connect-only retry as the only retry. |
| Malformed 200 that still carries `R'` | Persist `R'` with an expired `expires_at`, then raise | Discard the whole response | `R` is already consumed, so discarding `R'` guarantees the next tick replays `R` and loses the grant. |
| Re-auth write vs. in-flight refresh | Re-auth takes the same file lock | Unlocked write | An unlocked write can be overwritten by a refresh finishing on the old grant. |
| Cause chain on re-raised credential-path errors | Kept (`raise … from e`); only the new exception's message is reduced to the type name | Drop the chain (`from None`) | The Dagster event log stores and shows the cause chain, so in a rare case (an HTTP library quoting a malformed header) a credential could reach it. Accepted: these credentials grant read-only access to one person's data, and the chain is valuable for debugging failed runs. |
| Where credential-safe logging is enforced | Both: credential paths never log `exc_info`, and the core logger never renders locals | Only the core logger; only the call sites | Either layer alone has failed: the core setting existed but was never applied, and a stray `exc_info` dumped the request body. |
| Whoop client secret after its 2026-09-26 log exposure | Not rotated; the log lines expire with Loki retention (about 2026-10-26) | Rotate now | The grant's scopes are read-only, and the secret alone can't obtain data without the user's authorization or a live refresh token. Rotation can be done later at any time. |
| Recovery from a revoked grant | Manual re-auth (script plus runbook) | Automated re-auth | Re-auth needs an interactive browser consent. |

## Open Questions & Future Decisions

### Deferred

1. **Repeated failures against a dead grant.** Once a grant is revoked, every hourly tick
   still presents the dead refresh token (one attempt per run, more when bronze and
   snapshots runs line up). Suppressing refreshes after an `invalid_grant` until a re-auth
   would stop hammering Whoop's token endpoint. This is operational only; it doesn't change
   the outcome.
2. **Rotation lost to a failed write.** If Whoop rotates `R` and the atomic write of `R'`
   then fails (a full disk, or the process being killed), `R'` is lost and the grant dies.
   That can't be prevented in code. The write failure is raised and visible.

## References

- `docs/WHOOP_TOKEN_RUNBOOK.md`: symptoms, re-auth procedure, detection
- `packages/core/src/grecohome_core/tokens/file_lock.py`: cross-process lock
- `packages/core/src/grecohome_core/logging_config.py`: shared logger
