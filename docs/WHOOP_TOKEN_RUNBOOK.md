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
- `whoop_token_health` asset check (in `whoop_bronze_checks_job`) is the backstop: it
  ERRORs once `expires_at` is more than the grace window (`_TOKEN_GRACE_SECONDS`, ~90 min)
  in the past. Slower, but catches a stalled poller that isn't even attempting refreshes.

## Two ways the grant dies

There are two distinct failure modes behind a `400 invalid_grant`.

### 1. Server-side non-atomic rotation (rare)

Whoop's token rotation is non-atomic: once a refresh POST reaches Whoop, the refresh
token is consumed and rotated (`R -> R'`) even if the client never receives the
response. Two variants have bitten us:

- **Slow response / client timeout (2026-07-20).** The refresh used httpx's default
  **5s** timeout. Whoop's token endpoint is routinely slow (observed 1-4s); one refresh
  took >5s, so the read timed out *after* Whoop had rotated the token. `ReadTimeout` is a
  transport error, so the client retried and replayed the now-consumed `R` -> permanent
  `400`. **Fixed:** the token endpoint now uses a generous timeout
  (`_TOKEN_TIMEOUT = 30s`, 5s connect) so a slow-but-successful rotation completes and
  `R'` is persisted. **Tell-tale:** an `Unexpected error during token refresh` with an
  httpx timeout traceback right before the `400`s begin.
- **5xx during rotation (2026-06-11, again 2026-07-22).** A `5xx` can register/rotate the
  token server-side while still returning an error. Whoop rotates on *reuse detection*
  (RFC 6749): presenting the same refresh token twice revokes the whole grant. So the
  kill wasn't the `5xx` itself — it was the **retry replaying `R`** after it. On
  2026-07-22 a refresh got a `502` at 12s (the 30s timeout worked — no premature bail),
  the client retried, and the replayed `R` came back `400`. **Fixed:** the refresh now
  retries **only** connect-phase transport errors (`ConnectError`/`ConnectTimeout`/
  `PoolTimeout`), where the request provably never reached Whoop and `R` was never
  presented. Any HTTP response (4xx *or* 5xx) or a read/write timeout is re-raised
  without retry; a transient `5xx` fails one run and the next hourly tick recovers with
  `R` intact. **Tell-tale:** a single `Token refresh failed status_code=5xx`, then a
  `whoop_token_invalid_grant status_code=400` on the next attempt.

Recovery from an already-lost grant is still manual re-auth — no client change revives a
revoked grant.

### 2. Concurrent double-spend of the rotating refresh token (was the real cause, now fixed)

Whoop rotates the refresh token on every refresh and treats a *replayed* (already
consumed) refresh token as theft — revoking the **entire grant**. If two workers refresh
at once, both read refresh token `R`, both POST it; one wins (`R -> R'`), the other
replays the now-consumed `R` and the family is revoked. This caused the 2026-06-26 and
2026-07-20 outages. **Tell-tale:** clean `200`s (no 5xx), then at the top of an hour
several refreshes fire within seconds carrying *different* `time_until_expiry_seconds`
snapshots — separate processes refreshing independently.

Why the in-process guard wasn't enough: `TokenManager._refresh_locks` is an
`asyncio.Lock` (per event loop), but Dagster's multiprocess executor runs each step in
its own subprocess and `whoop_bronze_job` / `whoop_snapshots_job` are separate runs on
the same cron. **Fix (implemented):** `TokenManager.get_valid_token` now wraps the
refresh in a host-wide `fcntl.flock` file lock (`grecohome_core.tokens.file_lock`,
sentinel at `<WHOOP_TOKEN_PATH>.lock`) *inside* the asyncio lock, with a post-lock
re-read so the loser of a race reuses the just-rotated token instead of replaying the
consumed one. Belt-and-suspenders: keep the host `whoop_api` op-concurrency pool limit
at `1` in `dagster.yaml`.
