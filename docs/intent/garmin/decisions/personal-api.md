---
node: garmin
---

# Decision: Garmin Connect Personal API versus the `garminconnect` library

## Context

Garmin opened an official **Personal API** on 2026-10-21 (Garmin Connect Developer Program,
Personal tier). It is the first sanctioned way for an individual to read their own Garmin
Connect data programmatically. Until then this subject had no choice but the `garminconnect`
library, which signs in through Garmin's consumer SSO, and the authentication LLD's rationale
for delegating login to that library rested on there being no official alternative.

The Garmin bronze subject captures about 60 catalog collections plus ten per-activity fan-out
endpoints through `garminconnect`, and silver reads eight of those collections. The question
is whether to move capture onto the Personal API, in whole or in part, and when.

This doc records the evaluation made on 2026-11-02, the decision to defer, and the facts a
re-evaluation needs. The *Reference* section at the end is the snapshot of the Personal API
at evaluation time; refresh it before deciding again.

## Decision Elements

**Gates**

- **No silver table may lose its source.** `silver_sleep`, `silver_daily`, `silver_workouts`,
  `silver_workout_splits`, `silver_fitness` and `silver_body` all read Garmin bronze, and gold
  reads them. An option that drops a table's source without an agreed replacement fails.

**Criteria** *(importance: major / moderate / minor)*

- **Sanctioned access — major.** Does capture use an API Garmin publishes and supports, rather
  than the consumer SSO behind Cloudflare?
- **Fragility of the login path — major.** How likely is a Garmin-side change to stop capture,
  and how much of the pipeline stops when it does?
- **Data coverage — major.** How much of the current catalog, and in particular what silver
  consumes, remains capturable?
- **Operational simplicity — moderate.** Number of auth mechanisms, secrets, health checks and
  renewal chores.
- **Implementation cost — moderate.** New client, catalog changes, bronze shape changes, silver
  SQL changes, docs and specs.
- **Timing — moderate.** Is the API stable enough that work done now won't be redone?

## Options in the Domain

### Option A — Keep `garminconnect` for everything (status quo)

**Description.** No change. All collections continue through the library's SSO login,
token store and endpoint wrappers.

**Analysis.**
- *Sanctioned access (major):* **weak**. The library reaches the same backend service paths
  the Connect web app uses, through the consumer SSO. Its current releases depend on
  `curl-cffi` and `ua-generator`, which means browser TLS impersonation and randomized user
  agents to pass Cloudflare.
- *Fragility (major):* **weak**. Garmin changed its sign-in flow in early 2026 and the `garth`
  auth library the ecosystem relied on was retired. A repeat stops every Garmin collection at
  once. One commenter on the library's own tracker expects Garmin to police unofficial access
  more firmly now that an official route exists.
- *Data coverage (major):* **strong**. Everything in the catalog.
- *Operational simplicity (moderate):* **strong**. One mechanism, already built and guarded
  (token-store checks, health check, bootstrap).
- *Implementation cost (moderate):* **strong**. Zero.
- *Timing (moderate):* **strong**. Nothing is bet on an API two weeks old.

### Option B — Switch everything to the Personal API

**Description.** Replace the library with a thin client against the Personal API. Drop every
collection the Personal API doesn't serve.

**Analysis.**
- *Gate:* **fails**. The Personal API has no training status, VO2max (`max_metrics`), race
  predictions, training readiness, or per-activity splits, details, zones, weather, gear or
  exercise sets. `silver_fitness` and `silver_workout_splits` lose their sources, and
  `gold_daily_wellness` loses VO2max and training status.
- *Sanctioned access (major):* **strong**.
- *Fragility (major):* **strong**. Key plus 24-hour token, no SSO, no MFA, no Cloudflare.
- *Data coverage (major):* **weak**. Roughly 26 endpoints against the library's 300-plus; about
  35 bronze collections disappear and intraday stress, steps and respiration series become
  daily aggregates.
- *Operational simplicity (moderate):* **strong**. One static secret, one disposable token.
- *Implementation cost (moderate):* **weak**. New client, catalog rewrite, bronze shape changes
  (sleep, weight), silver SQL changes, check-config changes, retirement of two silver tables.
- *Timing (moderate):* **weak**. Garmin may add endpoints; a rewrite now would be rewritten.

### Option C — Hybrid: Personal API where it covers, library for the rest

**Description.** Add a Personal API client beside the library. Move the collections the
Personal API covers and silver consumes (`user_summary`, `activities`, `activity_summary`,
`activity_fit`, `sleep`, `hrv`, `heart_rates`, `body_battery`, weight) onto it. Keep the
library only for fitness metrics, splits and the long tail. The catalog gains a field naming
which client serves each endpoint.

**Analysis.**
- *Gate:* **passes**. Every silver source keeps a path.
- *Sanctioned access (major):* **partial**. The bulk of analytically important data moves to
  the sanctioned path; fitness and splits stay on the SSO path.
- *Fragility (major):* **partial**. An SSO break stalls `silver_fitness` and
  `silver_workout_splits` only; sleep, daily, workouts and body keep flowing. The dependency
  itself is not removed.
- *Data coverage (major):* **strong**. Nothing lost. Where the Personal API serves aggregates
  only (stress, steps, respiration), the library keeps the intraday collections.
- *Operational simplicity (moderate):* **weak**. Two auth mechanisms, two secrets, two health
  checks, a key renewal chore at most once a year, and a token mint step per run.
- *Implementation cost (moderate):* **partial**. New httpx client on core's capture framework,
  catalog field, sleep and weight bronze shape changes with matching silver SQL, a key-expiry
  health check, auth and capture LLD and spec updates. Smaller than B but not small.
- *Timing (moderate):* **partial**. The pieces that move are the stable, same-path endpoints,
  so later endpoint additions extend the hybrid rather than invalidate it. Still, the split
  line may move as Garmin extends the API.

## Selection

**Chosen: Option A for now, with Option C as the expected destination.**

B fails the gate outright. C is the right shape if a move is made, because it puts the data
gold depends on behind a supported API while keeping the fitness and splits tables alive.
But the API was two weeks old at evaluation, has undocumented rate limits, no refresh tokens
and a 26-endpoint allowlist, and the library maintainer declined to adopt it. Garmin may
extend the allowlist (the obvious additions are the metrics service and activity sub-resources),
in which case C's split line moves or disappears and work done now is redone. The current path
works and is guarded. Waiting costs nothing but exposure to an SSO change, and that exposure
already exists today.

**Re-evaluate when any of these happens:**

1. The Swagger allowlist or Garmin's Python client gains any of: `metrics-service`
   (training status, max metrics, race predictions, endurance or hill score, training
   readiness), `activity-service` sub-resources (splits, details, zones, weather, gear,
   exercise sets), or `device-service`. That removes most of the gap and pushes toward B.
2. The `garminconnect` SSO path breaks or Garmin starts blocking it. That forces C or B on
   whatever coverage exists at the time.
3. Garmin documents rate limits that our daily volume (about 14 partitions times roughly 50
   endpoints, plus fan-out) would exceed. That constrains C's design.
4. Six months pass without any of the above (about 2027-05). Re-read the *Reference* section
   against the live Swagger page and decide on the facts then.

## Reference: the Personal API as evaluated on 2026-11-02

Sources: the Personal API page, FAQ and Swagger page under
`developer.garmin.com/gc-developer-program/personal/`, the OpenAPI document embedded in the
Swagger page (version 1.0.0), Garmin's reference client `garmin/connect-personal-api-python`
(Apache-2.0), and the `cyberjunky/python-garminconnect` issue tracker.

### Program and terms

- Individual use only. "Not intended for commercial applications serving multiple end users."
  The key "may be used only by you" with tools "that you develop or operate for your own use."
- Free. Rate limits are undocumented. The Supplemental Terms let Garmin set and change request
  limits and "throttle, delay, reject, suspend, or otherwise restrict API requests without
  notice."
- Read-only: every endpoint is GET. No uploads, no edits.
- Base URL `https://personal.connectapi.garmin.com` (China: `personal.connectapi.garmin.cn`).
  The host serves only the allowlisted paths; others return 404. A Personal token is rejected
  by the regular `connectapi.garmin.com` host.

### Authentication

- Create a **Personal API key** at Garmin Connect web, Settings, Personal API Key. Choose an
  expiry of 30, 90, 180 or 365 days. The key is shown once; a lost key is deleted and recreated.
  Garmin emails about a week before expiry. Deleting a key invalidates its access tokens.
- Exchange the key for an access token:

  ```
  POST https://personal.connectapi.garmin.com/personalapitoken-service/personal-api/personal-api-key/token
  Authorization: ApiKey <personal-api-key>
  ```

  The response is an OAuth2 bearer token (a JWT, about 1.1 KB, with an `exp` claim).
- The access token is valid for **24 hours or until the key expires, whichever is first**.
- **Refresh tokens are not supported.** To get a new access token, call the token endpoint
  again with the same key. Re-minting is safe to repeat; nothing is single-use, unlike Whoop's
  rotating refresh token.
- Garmin's client caches the token on disk (mode 0600) and re-mints when under one hour of
  life remains, keyed on the JWT `exp`. A 401 on a data call is not retried; the caller
  re-mints and retries.
- Because token life is capped by key life, a freshly minted token with less than 24 hours
  remaining means the key is about to expire. That is a natural early-warning signal for a
  health check.

### Endpoints (all GET)

| Tag | Path | Parameters | Notes |
|---|---|---|---|
| Activity | `/activity-service/activity/{activityId}` | `activityId` | Summary, metadata, measurements, split summaries. Same path the library uses for `activity_summary`. |
| Activity | `/activity-service/activity/{activityId}/workouts` | `activityId` | Workouts performed during the activity. |
| Activity | `/activitylist-service/activities/search/activities` | `start`, `limit`, `startDate`, `endDate`, `activityType`, many filters | Paginated list. Same path the library uses for `activities`. About 45 per page. |
| Activity | `/download-service/files/activity/{activityId}` | `activityId` | Original FIT as ZIP bytes. No TCX, GPX, KML or CSV export. |
| Activity | `/wellnessactivity-service/activity/summary/range/{fromDate}/{untilDate}` | dates | Wellness activity summaries. |
| Health | `/biometric-service/heartRateZones/v2` | `generateDefaults` | HR zone configuration. |
| Health | `/fitnessage-service/fitnessage/v2/{calendarDate}` | `calendarDate`, `fullHistoryScan` | Fitness age. |
| Health | `/goal-service/goal/user/effective/weightgoal/{startDate}/{endDate}` | dates | Weight goals. |
| Health | `/hrv-service/hrv/{calendarDate}` | `calendarDate` | HRV summary and readings. Same path as the library's `hrv`. |
| Health | `/hrv-service/hrv/daily/{fromDate}/{toDate}` | dates | HRV daily summaries. |
| Health | `/nutrition-service/food/logs/{date}` | `date`, `includeGoals`, `includeNutrition`, `includeMedia`, `mealId` | Daily nutrition. |
| Health | `/sleep-service/sleep/dailySleep` | `date` | One night. Returns a flat `DailySleepDTO`. The library's `sleep` collection comes from `wellness-service/.../dailySleepData/{displayName}`, which wraps the same object as `dailySleepDTO` and adds sleep levels, movement and other series. |
| Health | `/sleep-service/sleep/dailySleepSummariesByDate` | `startDate`, `endDate` | Up to 31 days. |
| Health | `/usersummary-service/stats/hydration/daily/{startDate}/{endDate}` | dates | Up to 31 days. |
| Health | `/usersummary-service/stats/steps/daily/{startDate}/{endDate}` | dates | Daily totals, up to 28 days. Not the intraday series the library's `steps_intraday` captures. |
| Health | `/usersummary-service/stats/stress/daily/{startDate}/{endDate}` | dates | Daily aggregates, up to 28 days. Not the intraday series the library's `stress` captures. |
| Health | `/usersummary-service/usersummary/daily` | `calendarDate` | Daily user summary. Same path and shape as the library's `user_summary`. |
| Health | `/usersummary-service/v2/dailysummaries` | `startDate`, `endDate` | Up to one year. Compact per-day summary with stress, respiration, pulse-ox, HR, body battery, intensity minutes, movement, hydration sub-objects. |
| Health | `/weight-service/weight/all/{startDate}/{endDate}` | dates | All weight entries. |
| Health | `/weight-service/weight/range/{startDate}/{endDate}` | dates, `includeAll` | Daily latest weights plus averages (`dailyWeightSummaries`). Not the `dateWeightList` shape the library's `daily_weigh_ins` returns. |
| Health | `/wellness-service/wellness/bodyBattery/reports/daily` | `startDate`, `endDate`, `chunks` | Up to 31 days. Same path as the library's `body_battery`. |
| Health | `/wellness-service/wellness/daily/spo2acclimation/{calendarDate}` | `calendarDate` | Daily SpO2. The library's `spo2` uses `wellness/daily/spo2`. |
| Health | `/wellness-service/wellness/dailyHeartRate` | `date` | Same path as the library's `heart_rates`. |
| Women's Health | `/periodichealth-service/menstrualcycle/summary/{calendarDate}` | `calendarDate` | |
| Women's Health | `/periodichealth-service/menstrualcycle/dayview/{calendarDate}` | `calendarDate`, `todayDate`, `reducedResponse`, `skipbm`, `includeAnalysis` | |
| User Profile | `/userprofile-service/userprofile/settings` | | Same path as the library's `userprofile_settings`. |

### Coverage of this subject's catalog

| Catalog collection(s) | Silver consumer | Personal API |
|---|---|---|
| `user_summary` | `silver_daily` | Yes, same path and shape |
| `activities` | `silver_workouts` | Yes, same path |
| `activity_summary`, `activity_fit` | bronze only | Yes, same paths; FIT only |
| `hrv`, `heart_rates`, `body_battery`, `userprofile_settings` | bronze only | Yes, same paths |
| `sleep` | `silver_sleep` | Yes, different endpoint and flatter shape; silver's `dailySleepDTO.` prefix would go |
| `daily_weigh_ins`, `weigh_ins`, `body_composition` | `silver_body` | Partial; different shape |
| `spo2`, `fitness_age`, `hydration`, `nutrition_*`, `menstrual_*` | bronze only | Yes, some with different paths |
| `stress`, `steps_intraday`, `respiration`, `intensity_minutes`, `floors`, `daily_events`, `body_battery_events` | bronze only, several in `IMPORTANT` checks | Daily aggregates only, via stats or summary endpoints; intraday series not available |
| `max_metrics`, `training_status`, `race_predictions` | `silver_fitness`, then `gold_daily_wellness` | **No** |
| `training_readiness`, `morning_training_readiness`, `endurance_score`, `hill_score`, `running_tolerance`, `lactate_threshold`, `cycling_ftp` | bronze only | **No** |
| `activity_splits` | `silver_workout_splits` | **No** (`lapDTOs` not served; the activity summary's `splitSummaries` is a different, coarser object) |
| `activity_details`, `activity_typed_splits`, `activity_split_summaries`, `activity_weather`, `activity_hr_zones`, `activity_power_zones`, `activity_exercise_sets`, `activity_gear` | bronze only | **No** |
| `devices`, `device_last_used`, `primary_device`, `device_settings`, `device_solar`, `device_alarms` | bronze only | **No** |
| `gear`, `gear_defaults`, `goals`, `personal_records`, `training_plans`, `workouts`, `*_badges`, `weekly_*`, `activity_types`, `user_settings`, `pregnancy_summary`, `lifestyle_logging`, `blood_pressure` | bronze only | **No** |

### Ecosystem signals

- Garmin's reference client (`garmin-connect-personal-api-python` on PyPI) is Python 3.12+,
  `requests`-based, Apache-2.0: token mint with on-disk cache, GET wrappers for the endpoints
  above, a generic `get()` for the rest, no retries, whole FIT ZIP held in memory.
- `cyberjunky/python-garminconnect` issue #476 ("Garmin Personal API Available",
  2026-10-21) was closed by the maintainer on 2026-10-23 as unusable for that library ("only
  35 datapoints compared with 300+") with a pointer to Garmin's client. No upstream migration
  is planned.
- Independent probes (for example `tamcore/garmin-mcp` issue #42, 2026-10-22) confirm: 26
  endpoints, unlisted paths 404, token rejected on the regular host, HRV shape identical to
  the unofficial API, sleep and stress shapes different.

### Design notes for Option C, if taken

- Write a small httpx client on core's capture framework rather than depending on Garmin's
  `requests`-based package. Mint the token with the key; cache it beside the key; re-mint on
  401 once.
- Store the key as a plaintext secret at a path like `WHOOP_TOKEN_PATH`, never under
  `BRONZE_ROOT`, mounted from the host secrets directory.
- Add a key-expiry health check: fail when a freshly minted token's `exp` is less than about
  seven days out, or when the mint returns 401.
- Keep the existing 2-second pacing and the `garmin_api` pool until Garmin documents limits.
- Give each catalog `Endpoint` a client field (`personal` or `library`) and let the Dagster
  resource build both clients; only `library` endpoints need the SSO login.
- Migrating `sleep` changes the bronze payload shape; silver's Garmin sleep SQL must accept
  both the wrapped (`dailySleepDTO`) and flat forms, since bronze is append-only and old
  captures stay.
- This is an HLD-level change (new auth path, new premise for delegating login). Start at the
  Garmin sub-HLD and the authentication LLD.
