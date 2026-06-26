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
Token refresh failed           status_code=400        # 400 == invalid_grant == dead refresh token
whoop_token_invalid_grant      status_code=400        # the distinct terminal signal
```

A 5xx (`status_code=503`) burst instead means transient Whoop instability — the client
retries those automatically (3 attempts, backoff). Only a 400/401 is terminal.

## Recover (manual re-auth)

Run the OAuth flow on the host and write a fresh token. It's a server, so use headless
mode (authorize in a browser on any device, paste the callback URL back):

```
python -m grecohome_whoop.oauth_setup --headless
```

This writes a new access+refresh token atomically to `WHOOP_TOKEN_PATH`. No restart is
needed — the file is re-read each run. The next hourly tick recovers; re-run any failed
runs to backfill.

## Detection

- `whoop_token_invalid_grant` (ERROR) fires on the **first** failed refresh — page on it
  for immediate re-auth.
- `whoop_token_health` asset check (in `whoop_bronze_checks_job`) is the backstop: it
  ERRORs once `expires_at` is more than the grace window (`_TOKEN_GRACE_SECONDS`, ~90 min)
  in the past. Slower, but catches a stalled poller that isn't even attempting refreshes.

## Why it can't be prevented in code

Whoop's token rotation is non-atomic under load: a 5xx during a refresh can rotate the
token server-side while returning an error, so the client never receives the new token
and is left holding a consumed one. See the `whoop-token-503-rotation-incident` history.
The client mitigates the *transient* variant (bounded 5xx/network retry) and detects the
terminal one fast, but recovery from a lost grant is always manual re-auth.
