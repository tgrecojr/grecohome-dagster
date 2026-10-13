---
parent: garmin
prefix: GARMIN-AUTH
---

# Garmin authentication

## Context

Every Garmin run logs in before it can capture anything. A login that resumes from the
saved session is cheap; a full password login goes through Garmin's SSO, which rate-limits
and Cloudflare-blocks bursts. This component keeps logins on the cheap path and makes it
loud when they can't be.

Authentication is delegated to `garminconnect`; this package never reimplements Garmin's
login (Cloudflare challenges, MFA, token refresh). Each run builds one client as a Dagster
resource and logs in once. Every op in the run shares that client through the in-process
executor. Logins happen during resource setup, before any op claims the `garmin_api` pool,
so the pool doesn't limit concurrent logins.

## Resume vs. password login

`GARMINTOKENS` names the library's token store, and the library resolves the token file
from it (`garminconnect.client.token_file_path`). This package uses that same function
rather than reimplementing the rule.
- A directory, or any path not ending `.json`, means the file `<GARMINTOKENS>/garmin_tokens.json`.
- A path ending `.json` is the file itself.
- A symlink anywhere in the path is refused.
- Inline token JSON in `GARMINTOKENS` is also accepted by the library, but it can never be
  saved back, so this package treats it as an unusable store.

The library saves the file `0600` inside a `0700` directory. On a bind mount, that also
changes the host directory's mode.

A login first **resumes** from the token file: a cheap token load, a refresh if the session
is close to expiry, and the profile and settings calls that every login makes. Only when no
usable session exists does it do a **full password login**, then save the new session.

Every daily tick starts about `LOOKBACK_DAYS` partition runs, plus the reference run, and
each logs in. With a working store those are resumes. Without one, every run does a full
password login through Garmin's SSO, which Garmin rate-limits (429) and Cloudflare-blocks
after a burst of roughly a dozen. Runs past that point fail at login.

## Making a broken store loud

The library fails **silently** when the store can't be used: a failed load is logged only
at debug level, and a failed save is swallowed. So a misconfigured store looks healthy while
every run does a password login. The login wrapper checks the store **once per run**, outside
the login retry, so a retried login doesn't repeat the checks.

**Before the login.** Log `garmin_tokenstore_unusable` (ERROR) with the path and a reason
when any of these holds:

| Reason | Condition |
|---|---|
| `inline_json` | `GARMINTOKENS` holds token JSON instead of a path |
| `path_rejected` | the library refuses the path: a symlink anywhere in it, or another path it won't use (such as `~otheruser`) |
| `not_writable` | the token file's directory exists but isn't a writable, searchable directory for this process |
| `parent_not_writable` | the token file's directory is missing and its nearest existing parent isn't writable |
| `off_volume` | no directory above the token file (other than `/`) is a mount point: the session lives on the container's own filesystem and is lost on the next redeploy, even after the library has created the directory |

The login then continues. A misconfigured store makes logins expensive, but it shouldn't
stop capture on its own (*Completeness over economy*).

**After the login.** Log `garmin_tokenstore_not_saved` (ERROR) unless the token file exists
and either holds exactly the session the client now has in memory (compared with the
library's `dumps()`), or was modified at or after this login started. The second condition
covers concurrent runs: when two runs both refresh a nearly expired session, the last save
wins, and the other run's session legitimately differs from the file. A fresh save by any run
still proves the store is writable and current. Comparing contents, not just existence,
catches a read-only store that still holds an old file. The contents are compared in memory
and never logged.

**`garmin_tokenstore_health` asset check** (ERROR; posts to Slack, logs
`garmin_tokenstore_unhealthy`). It's attached to `garmin_bronze_sleep` and runs hourly in
`garmin_bronze_checks_job` (and after each daily partition run). It fails when:
- the store meets any condition in the table above,
- the token file is missing or unreadable, or
- the token file isn't JSON containing the session fields (`di_token`, `di_refresh_token`,
  `di_client_id`).

It reports only the path and the reason, never the contents. It doesn't judge how recent
the file is: the library rewrites it only when it refreshes the session, and Garmin's token
lifetime isn't documented.

**The bootstrap** (`bootstrap.py`, the interactive first login, which may need MFA) runs the
same post-login check. It exits non-zero if the session wasn't saved, instead of reporting
success.

## Deployment rule

`GARMINTOKENS` must be the **mounted, writable token directory**:

- In the standard deployment that's `/secrets/garmin`, the code default, mounted from the
  host's `/opt/docker/dagster/garmin/secrets`.
- It must be writable by the container's user, with no symlink anywhere in the path.
- It must never be under `BRONZE_ROOT`.
- The bootstrap must mount the same host directory as the running container.

## Decisions & Alternatives

| Decision | Chosen | Alternatives Considered | Rationale |
|---|---|---|---|
| Unusable token store | Log ERROR once per run before login (`garmin_tokenstore_unusable`) and after it (`garmin_tokenstore_not_saved`), then keep logging in | Fail the run; rely on the library's own (debug-level) logging | Failing would stop capture over a cost problem. Silence let a wrong `GARMINTOKENS` run unnoticed for months until the login volume tripped Garmin's rate limit. |
| How to tell the session was saved | File contents equal the client's in-memory session (`dumps()`) | The file exists | A read-only store keeps an old file, so existence passes while every run does a password login whose save is swallowed. |
| Token-file path resolution | Use the library's `token_file_path` | Reimplement the suffix and symlink rules | `garminconnect` is pinned `==`; reimplementing would drift from what the library actually loads and saves. |
| Token-store health check | Store usable, plus a readable token file with the session fields | Also require a recent modification time; plain "valid JSON" | The library rewrites the file only on session refresh, at an undocumented interval, so a recency threshold would either never fire or fire falsely. `{}` is valid JSON that no login can use. |
| Run shape | One job, in-process executor, one shared client per run | Multiprocess executor with per-op logins | One login per run avoids repeated Garmin SSO logins and the MFA and rate-limit pressure they cause. |

## References

- `packages/garmin/src/grecohome_garmin/auth.py`, `bootstrap.py`, `dagster/resources.py`
- `garminconnect` (`Garmin.login`, `client.token_file_path`, `client.dump`/`load`)
