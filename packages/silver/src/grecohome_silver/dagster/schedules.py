"""Jobs + schedules for the silver code location.

One daily job rebuilds all three sleep assets (the unified table's in-location deps
order it after the two sources). A second, checks-only job runs the silver checks
independently — so a *stopped* silver asset is still caught — reusing the core
checks-job builders. Both jobs carry **no concurrency pool**: silver makes no source
API calls and must never contend with the ``*_api`` ingestion pools.

The rebuild runs daily a few hours after the day's bronze sleep lands (Garmin daily
+ Whoop hourly captures). Silver is a whole-table projection of current bronze, so a
daily cadence is sufficient — there is nothing intraday to chase.
"""

from __future__ import annotations

from dagster import ScheduleDefinition, define_asset_job

from grecohome_core.checks import build_bronze_checks_job, build_bronze_checks_schedule
from grecohome_silver.dagster.assets import SLEEP_ASSETS
from grecohome_silver.dagster.checks import SLEEP_CHECKS
from grecohome_silver.dagster.glucose_assets import GLUCOSE_ASSETS
from grecohome_silver.dagster.glucose_checks import GLUCOSE_CHECKS
from grecohome_silver.dagster.workout_assets import WORKOUT_ASSETS
from grecohome_silver.dagster.workout_checks import WORKOUT_CHECKS

ALL_CHECKS = SLEEP_CHECKS + GLUCOSE_CHECKS + WORKOUT_CHECKS

# Daily rebuild of the three sleep assets (source intermediates + unified table).
silver_sleep_job = define_asset_job("silver_sleep_job", selection=SLEEP_ASSETS)

silver_sleep_daily = ScheduleDefinition(
    name="silver_sleep_daily",
    job=silver_sleep_job,
    cron_schedule="0 6 * * *",  # 06:00 UTC — after the day's bronze sleep has landed
    execution_timezone="UTC",
)

# Daily rebuild of the glucose table (Lingo arrives via sensor; a daily rebuild keeps
# silver a current projection without chasing each upload).
silver_glucose_job = define_asset_job("silver_glucose_job", selection=GLUCOSE_ASSETS)

silver_glucose_daily = ScheduleDefinition(
    name="silver_glucose_daily",
    job=silver_glucose_job,
    cron_schedule="30 6 * * *",  # 06:30 UTC
    execution_timezone="UTC",
)

# Daily rebuild of the workouts table (Garmin activities).
silver_workouts_job = define_asset_job("silver_workouts_job", selection=WORKOUT_ASSETS)

silver_workouts_daily = ScheduleDefinition(
    name="silver_workouts_daily",
    job=silver_workouts_job,
    cron_schedule="45 6 * * *",  # 06:45 UTC
    execution_timezone="UTC",
)

# One checks-only job + daily schedule across all silver tables (the tables rebuild
# daily, so hourly checks would add nothing). Catches a silver asset that stops
# materializing.
silver_checks_job = build_bronze_checks_job(ALL_CHECKS, name="silver_checks_job")
silver_checks_schedule = build_bronze_checks_schedule(
    silver_checks_job, name="silver_checks_daily", cron_schedule="0 7 * * *"
)
