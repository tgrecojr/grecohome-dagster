# Whoop OAuth token runbook

The Whoop bronze pipeline authenticates with a rotating OAuth refresh token. Whoop
**rotates the refresh token on every refresh**, so the token file at `WHOOP_TOKEN_PATH`
(`/secrets/whoop/token.json` on host `fridge`) always holds exactly one live grant.
If that grant is ever consumed or revoked server-side, **only re-auth recovers it** —
no client-side retry can.

## Symptom

Every hourly `whoop_bronze_job` / `whoop_snapshots_job` tick fails on auth. In Loki
(`{service_name="dagster_whoop"}`):

```
whoop_token_invalid_grant      status_code=400        # dead refresh token (400/401) -- re-auth
Failed to refresh token        error_type=HTTPStatusError status_code=400
```

A 5xx (`status_code=502`/`503`) on a refresh means transient Whoop instability. The
refresh does **not** retry it (retrying would replay the single-use token and revoke the
grant — see failure mode 1); it fails that one run and the next hourly tick recovers with
the refresh token intact. A `400`/`401` is the only terminal signal.

## Recover (manual re-auth)

Run the headless OAuth flow on host `fridge` and write a fresh token (authorize in a
browser on any device, paste the callback URL back):

```
scripts/whoop-reauth.sh
```

The pipeline runs as a gRPC code location with no shell of its own, so the script spins up
a throwaway container from the **same image**, **volumes**, and **env** as the running
`dagster_whoop` container and runs `grecohome_whoop.oauth_setup --headless` inside it —
writing the new token to the real `WHOOP_TOKEN_PATH` the pipeline reads. It cleans up the
temporary env file (which carries `WHOOP_CLIENT_SECRET`) on exit. Override the container or
runtime user with `WHOOP_CONTAINER` / `WHOOP_RUN_USER` if they differ.

Equivalent by hand, if you can't use the script:

```
docker inspect dagster_whoop --format '{{range .Config.Env}}{{println .}}{{end}}' > /tmp/whoop.env
docker run --rm -it --entrypoint python \
  --user 1000:988 \
  --env-file /tmp/whoop.env \
  --volumes-from dagster_whoop \
  "$(docker inspect dagster_whoop --format '{{.Config.Image}}')" \
  -m grecohome_whoop.oauth_setup --headless
rm -f /tmp/whoop.env   # holds WHOOP_CLIENT_SECRET
```

Either way writes a new access+refresh token atomically to `WHOOP_TOKEN_PATH`. No restart
is needed — the file is re-read each run. The next hourly tick recovers; re-run any failed
runs to backfill.

## Detection

- `whoop_token_invalid_grant` (ERROR) fires on the **first** failed refresh — page on it
  for immediate re-auth.
- `whoop_token_health` asset check (in `whoop_bronze_checks_job`, hourly at `:00`) is the
  backstop: it ERRORs once `expires_at` is more than the grace window
  (`_TOKEN_GRACE_SECONDS`, ~90 min) in the past, or when the token file is missing or
  unreadable. It logs `whoop_token_unhealthy` (and `whoop_token_file_unreadable` for a
  corrupt file). Slower, but catches a stalled poller that isn't even attempting refreshes.
- Errors on the token and API paths log `error_type` (and `status_code`), never a
  traceback or the exception text -- those can carry the client secret or a token.

## How a grant dies, and the guard for each

Whoop's refresh token is single-use. Once a refresh POST reaches Whoop, `R` is consumed
and rotated to `R'` even if we never receive the response, and presenting a consumed `R`
again trips Whoop's **reuse detection**, which revokes the whole grant. Every known way to
lose the grant is a variant of "`R` presented twice" or "`R'` lost". The authoritative
design is `docs/intent/whoop/auth/auth-design.md`; this section is the field guide.

| Failure | Guard | Tell-tale in Loki |
|---|---|---|
| **Retrying a refresh that reached Whoop** (2026-06-11 503s, 2026-07-22 502) | The refresh retries **only** connect-phase errors (`ConnectError`/`ConnectTimeout`/`PoolTimeout`); any HTTP response or read/write timeout is raised without retry. A token failure reaches the API client as a non-retryable `WhoopAuthError`, so the data-request retry can't re-run it either. | One `Token refresh failed status_code=5xx` (or `Unexpected error during token refresh error_type=ReadTimeout`), then `whoop_token_invalid_grant status_code=400` on the next tick. |
| **Read timeout after rotation** (2026-07-20) | 30 s token-endpoint timeout (5 s connect), so a slow but successful rotation completes and `R'` is saved. | `Unexpected error during token refresh error_type=ReadTimeout` right before the `400`s. |
| **Two processes refreshing at once** (2026-06-26) | Host-wide `fcntl` lock at `<WHOOP_TOKEN_PATH>.lock` plus a re-read after locking, so a waiter reuses the token the holder just rotated. Re-auth takes the same lock. | Several `Token near expiration, refreshing` lines within seconds with *different* `time_until_expiry_seconds`. |
| **5xx mid-rotation at the top-of-hour surge** (deaths at `HH:00`, through 2026-07-24) | Both Whoop schedules run at minute **17**. None lost since. | A `5xx` then `400`, at `HH:17`. If this recurs, open a Whoop support ticket with the traces and consider a slower poll cadence. |
| **Malformed 200** (no `access_token`/`expires_in`) | A rotated `R'` in the response is saved, marked expired, so the next caller refreshes with `R'`. | `whoop_token_malformed_refresh_kept_rotation` (WARNING). |
| **200 with no rotated refresh token** | The current token is kept and the anomaly is logged. | `whoop_token_no_rotation` (WARNING). |

Recovery from an already-revoked grant is always manual re-auth (above). No client
change revives a revoked grant.
