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
from grecohome_silver.dagster.body_assets import BODY_ASSETS
from grecohome_silver.dagster.body_checks import BODY_CHECKS
from grecohome_silver.dagster.checks import SLEEP_CHECKS
from grecohome_silver.dagster.daily_assets import DAILY_ASSETS
from grecohome_silver.dagster.daily_checks import DAILY_CHECKS
from grecohome_silver.dagster.fitness_assets import FITNESS_ASSETS
from grecohome_silver.dagster.fitness_checks import FITNESS_CHECKS
from grecohome_silver.dagster.glucose_assets import GLUCOSE_ASSETS
from grecohome_silver.dagster.glucose_checks import GLUCOSE_CHECKS
from grecohome_silver.dagster.recovery_assets import RECOVERY_ASSETS
from grecohome_silver.dagster.recovery_checks import RECOVERY_CHECKS
from grecohome_silver.dagster.strain_assets import STRAIN_ASSETS
from grecohome_silver.dagster.strain_checks import STRAIN_CHECKS
from grecohome_silver.dagster.weather_assets import WEATHER_ASSETS
from grecohome_silver.dagster.weather_checks import WEATHER_CHECKS
from grecohome_silver.dagster.workout_assets import WORKOUT_ASSETS
from grecohome_silver.dagster.workout_checks import WORKOUT_CHECKS
from grecohome_silver.dagster.workout_splits_assets import WORKOUT_SPLITS_ASSETS
from grecohome_silver.dagster.workout_splits_checks import WORKOUT_SPLITS_CHECKS

ALL_CHECKS = (
    SLEEP_CHECKS + GLUCOSE_CHECKS + WORKOUT_CHECKS + RECOVERY_CHECKS
    + WEATHER_CHECKS + DAILY_CHECKS + STRAIN_CHECKS + BODY_CHECKS + FITNESS_CHECKS
    + WORKOUT_SPLITS_CHECKS
)

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

# Daily rebuild of the recovery table (Whoop recovery).
silver_recovery_job = define_asset_job("silver_recovery_job", selection=RECOVERY_ASSETS)

silver_recovery_daily = ScheduleDefinition(
    name="silver_recovery_daily",
    job=silver_recovery_job,
    cron_schedule="50 6 * * *",  # 06:50 UTC
    execution_timezone="UTC",
)

# Daily rebuild of the daily-summary table (Garmin user_summary; after the daily capture).
silver_daily_job = define_asset_job("silver_daily_job", selection=DAILY_ASSETS)

silver_daily_daily = ScheduleDefinition(
    name="silver_daily_daily",
    job=silver_daily_job,
    cron_schedule="40 6 * * *",  # 06:40 UTC
    execution_timezone="UTC",
)

# Daily rebuild of the strain table (Whoop cycle; after the Whoop hourly capture).
silver_strain_job = define_asset_job("silver_strain_job", selection=STRAIN_ASSETS)

silver_strain_daily = ScheduleDefinition(
    name="silver_strain_daily",
    job=silver_strain_job,
    cron_schedule="52 6 * * *",  # 06:52 UTC
    execution_timezone="UTC",
)

# Daily rebuild of the weather table (USCRN hourly; bronze captured a few times a day,
# a daily rebuild keeps silver a current projection without chasing each capture).
silver_weather_job = define_asset_job("silver_weather_job", selection=WEATHER_ASSETS)

silver_weather_daily = ScheduleDefinition(
    name="silver_weather_daily",
    job=silver_weather_job,
    cron_schedule="55 6 * * *",  # 06:55 UTC
    execution_timezone="UTC",
)

# Daily rebuild of the body table (Garmin weigh-ins; after the daily capture).
silver_body_job = define_asset_job("silver_body_job", selection=BODY_ASSETS)

silver_body_daily = ScheduleDefinition(
    name="silver_body_daily",
    job=silver_body_job,
    cron_schedule="42 6 * * *",  # 06:42 UTC
    execution_timezone="UTC",
)

# Daily rebuild of the fitness-snapshot table (Garmin VO2max / status / race predictions).
silver_fitness_job = define_asset_job("silver_fitness_job", selection=FITNESS_ASSETS)

silver_fitness_daily = ScheduleDefinition(
    name="silver_fitness_daily",
    job=silver_fitness_job,
    cron_schedule="44 6 * * *",  # 06:44 UTC
    execution_timezone="UTC",
)

# Daily rebuild of the workout-splits table (Garmin per-lap detail; after the daily capture).
silver_workout_splits_job = define_asset_job(
    "silver_workout_splits_job", selection=WORKOUT_SPLITS_ASSETS
)

silver_workout_splits_daily = ScheduleDefinition(
    name="silver_workout_splits_daily",
    job=silver_workout_splits_job,
    cron_schedule="47 6 * * *",  # 06:47 UTC
    execution_timezone="UTC",
)

# One checks-only job + daily schedule across all silver tables (the tables rebuild
# daily, so hourly checks would add nothing). Catches a silver asset that stops
# materializing.
silver_checks_job = build_bronze_checks_job(ALL_CHECKS, name="silver_checks_job")
silver_checks_schedule = build_bronze_checks_schedule(
    silver_checks_job, name="silver_checks_daily", cron_schedule="0 7 * * *"
)
