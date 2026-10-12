---
parent: high-level-design
prefix: WHOOP
---

# Whoop bronze subject

## Context and Design Philosophy

`grecohome_whoop` is the bronze code location for the Whoop API. It captures four daily
range collections (`sleep`, `recovery`, `workout`, `cycle`) and two current-only snapshots
(`profile`, `body_measurement`) to `BRONZE_ROOT/whoop/`, using the core writer with
skip-identical on. Whoop rescores and deletes records after the fact, so recent days are
re-captured every hour across a trailing reconcile window.

The hard part of this subject is **authentication**, not capture. Whoop uses OAuth 2.0 with
a **single-use, rotating refresh token**:

- Every refresh consumes the current refresh token `R` and returns a new one, `R'`.
- Presenting a consumed `R` a second time trips Whoop's **reuse detection**, which revokes
  the whole grant.
- A revoked grant is unrecoverable in code. Only an interactive browser re-auth restores it.

Losing a grant stops all Whoop capture until a person notices and re-authenticates. So the
token design leans hard toward **never presenting a refresh token twice**. When that rule
conflicts with availability, it wins: a refresh that might have reached Whoop is never
retried, even if that means a run fails.


## Components

The subject has two parts, each with its own design doc and specs:

| Component | Doc | Spec prefix | Owns |
|---|---|---|---|
| Token lifecycle | [auth/auth-design.md](auth/auth-design.md) | `WHOOP-AUTH` | OAuth token storage, refresh, serialization, retry rule, re-auth; credential-safe logging; the token-health check |
| Capture | [capture/capture-design.md](capture/capture-design.md) | `WHOOP-CAPTURE` | Schedules, range and snapshot capture, data-request retry and rate limit, pagination, the bronze collection checks |

The two meet at one point: the API client obtains its bearer token from the token manager
before every data request. A failure there is a non-retryable `WhoopAuthError` (owned by the
token lifecycle), which the capture retry must never retry.

## References

- `docs/high-level-design.md`: *Logs never carry credentials*; bronze re-capture
- `docs/WHOOP_TOKEN_RUNBOOK.md`: symptoms, re-auth procedure, detection
