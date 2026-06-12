# Observability & Alerting — implementation plan (not yet built)

> **Status: PLAN.** Captured 2026-06-12 to implement next. Nothing here is built yet.
> Scope: proactive detection + **push alerts to Slack** for the data platform, starting
> with Whoop auth health and generalizing to all layers. Driven by two silent multi-hour
> incidents — the [token 503 rotation outage](adr/) and the pagination pool-wedge — that
> were only caught by eyeballing the UI.

## Why

Self-hosted Dagster **OSS has no native push alerting** (that's Dagster+). Today an asset
check or a failed run only shows **red in the Dagster UI** — passive. We already ship
structlog JSON to **Loki** (`{service_name="dagster_<subject>"}`) and run **Grafana**
(Loki datasource `uid P8E80F9AEF21F6940`). So the push path is: **emit a signal → Grafana
Loki alert rule → Slack contact point.** This plan adds the signals (in-repo) and the rules
(Grafana), and calls out platform-level safeguards.

## Current state (what we already have)

- **Asset checks** across all layers (bronze freshness/completeness/schema/content;
  silver/gold uniqueness/ranges/coverage) — red in UI, no push.
- **Logs → Loki**, structured JSON, per code location.
- **Grafana** with the Loki datasource; **one contact point today: "email receiver"** (we
  want Slack instead).
- **Gaps:** no push notifications; no stuck-run safeguard (the wedge ran 2.7h); no
  queue-depth signal; no token-health signal; no platform dashboard.

## Slack wiring (prerequisite, host/Grafana side)

The Slack contact point holds a **secret** (webhook URL or bot token), so it is created in
Grafana by the operator — not via the MCP, and the secret is never pasted into a session.

1. Slack: add an **Incoming Webhook** to the target channel → copy the
   `https://hooks.slack.com/services/…` URL (or create a Slack app with `chat:write` + channel).
2. Grafana: **Alerting → Contact points → Add → type "Slack"** → paste webhook (or token +
   channel) → Test → Save. Note its **name** (e.g. `slack-grecohome`).
3. Route to it (per-rule contact point, or a notification policy matcher like `service=whoop`).

Alert **rules** carry no secret, so they can be created via the Grafana MCP
(`alerting_manage_rules`) pointing at the Slack contact point — or pasted from the specs below.

---

## Phase 1 — Whoop token health (highest value; the recurring pain)

**1a. In-repo asset check `whoop_token_health`**
- New `@asset_check` (likely on `whoop_bronze_sleep` or a dedicated key), added to
  `whoop_bronze_checks_job` (hourly, **off the `whoop_api` pool**, **no Whoop API call**).
- Logic: read the token file at `WHOOP_TOKEN_PATH` (`/secrets/whoop/token.json`) and **fail
  ERROR** if `expires_at` is in the past beyond a grace window. A healthy pipeline refreshes
  hourly, so `expires_at` is always ~1h out; if it's stale, refresh is broken. Also fail if the
  file is missing/unparseable.
- On failure: red in the Dagster UI **and** a distinct structured log line,
  `event="whoop_token_unhealthy"`, with `expires_at` / age, to Loki.
- Threshold: grace ≈ refresh cadence + buffer (start ~90 min). Tune.
- Tests: expired token → ERROR; fresh token → pass; missing/unparseable → ERROR.
- Files: `packages/whoop/src/grecohome_whoop/dagster/checks.py` (+ a small token-file reader,
  or reuse `token_manager`/`file_store` read path), tests in `packages/whoop/tests/`.

**1b. Grafana Loki alert rules → Slack** (two signals; wire both)
- **Refresh-failure (fastest, Dagster-independent — fires at refresh time):**
  `count_over_time({service_name="dagster_whoop"} |~ "Token refresh failed|invalid_grant" [3h]) > 2`
- **Token-health (the check's signal):**
  `count_over_time({service_name="dagster_whoop"} |= "whoop_token_unhealthy" [90m]) > 0`
- Both route to the Slack contact point; annotate with a one-line runbook
  (`python -m grecohome_whoop.oauth_setup` to reauth).

---

## Phase 2 — Generic capture/run-failure alerting (all code locations)

Catch *any* failed materialization or error, not just Whoop auth.
- **Loki rule per (or across) code location:** alert on Dagster error/failure log lines, e.g.
  `count_over_time({service_name=~"dagster_.+"} | json | level="ERROR" [1h]) > 0` (tune the
  matcher to the actual error shape) → Slack, grouped by `service_name`.
- Complements the existing **freshness** asset checks (which already detect a *stopped* asset);
  surface those to Slack too via their log signal.

## Phase 3 — Stuck-run & queue safeguards (platform-level)

The pagination wedge showed the other failure mode: a run **STARTED for hours**, backing up the
queue. Two defenses, independent of any one bug:
- **Dagster run monitoring (`run_monitoring` in `dagster.yaml`, host-owned):** set a
  `max_runtime` (per-run via the `dagster/max_runtime` tag, or instance default) so the
  monitoring daemon **auto-terminates** a run exceeding it. This would have killed the wedged
  workout run automatically. *Defense in depth alongside the pagination code fix.* Host/Ansible
  change to the mounted `dagster.yaml`.
- **Queue-depth alert:** notify when `queued_runs_count` stays high. Dagster OSS doesn't export
  this to Prometheus by default; options: (a) a small scheduled "platform-health" check that
  queries the instance and logs a metric line Loki can alert on, or (b) Grafana
  Infinity/JSON datasource hitting the Dagster GraphQL `instance.runQueueConfig`/queued count.
  Decide the metrics source.

## Phase 4 — Platform dashboard (Grafana)

A single "data platform health" dashboard:
- Per-subject **last successful capture** time (from freshness signals / Loki).
- **Run success/failure** counts per code location; **queue depth** over time.
- **Asset-check** pass/fail counts (bronze/silver/gold).
- Bronze→silver→gold **row counts** over time (silver/gold log row counts in materialization
  metadata; could also be derived).
- Token `expires_at` recency for authed subjects.

## Phase 5 — Generalize auth/health to other subjects

The token-health pattern extends to the other credentialed sources:
- **Garmin** — `garminconnect` token store at `GARMINTOKENS`; health = store present + recent.
- **Lingo** — Google service-account key presence + Drive list succeeding.
- **Soil** — no auth (public NOAA); only capture-failure/freshness applies.
Consider a shared `*_auth_health` check pattern in `grecohome_core.checks`.

---

## Priority / sequencing
1. **Phase 1** (Whoop token health + refresh-failure Slack alert) — the recurring incident.
2. **Phase 2** (generic run-failure → Slack).
3. **Phase 3** (`run_monitoring` max_runtime + queue-depth alert) — kills the stuck-run class.
4. **Phase 4** (dashboard).
5. **Phase 5** (other subjects' auth health).

## Division of labor
- **Repo (code, PRs):** asset checks + structured log signals, tests, this doc → eventually a
  proper `docs/OBSERVABILITY.md` once built.
- **Host / Ansible:** `dagster.yaml` `run_monitoring` (Phase 3); Slack contact-point secret.
- **Grafana (operator UI, or me via MCP for the no-secret rules):** Slack contact point,
  Loki alert rules, dashboard.

## Open decisions to settle before building
- Slack channel + **contact-point name** to target.
- Thresholds: token-health grace (~90 min?), refresh-failure count/window (>2 in 3h?),
  `max_runtime` for run monitoring.
- Metrics source for queue depth (platform-health check vs Grafana JSON-over-GraphQL).
- Whether Phase 1's check attaches to an existing whoop asset or a dedicated synthetic asset.

## Related
- Incidents: `whoop-token-503-rotation-incident`, `whoop-pagination-pool-wedge` (memory);
  pagination fix PR #51.
- [VALIDATION](VALIDATION.md) (bronze checks + the checks-only-job pattern this builds on),
  [DEPLOYMENT](DEPLOYMENT.md) (host `dagster.yaml`, Loki/`service_name` logging).
