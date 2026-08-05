"""Dagster Definitions for the silver code location (the gRPC server target).

Served by ``dagster code-server start -m grecohome_silver.dagster.definitions`` and
registered with the host daemon/webserver via the host's ``workspace.yaml``.
"""

from dagster import Definitions

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
from grecohome_silver.dagster.schedules import (
    silver_body_daily,
    silver_body_job,
    silver_checks_job,
    silver_checks_schedule,
    silver_daily_daily,
    silver_daily_job,
    silver_fitness_daily,
    silver_fitness_job,
    silver_glucose_daily,
    silver_glucose_job,
    silver_recovery_daily,
    silver_recovery_job,
    silver_sleep_daily,
    silver_sleep_job,
    silver_strain_daily,
    silver_strain_job,
    silver_weather_daily,
    silver_weather_job,
    silver_whoop_workouts_daily,
    silver_whoop_workouts_job,
    silver_workout_splits_daily,
    silver_workout_splits_job,
    silver_workouts_daily,
    silver_workouts_job,
)
from grecohome_silver.dagster.strain_assets import STRAIN_ASSETS
from grecohome_silver.dagster.strain_checks import STRAIN_CHECKS
from grecohome_silver.dagster.weather_assets import WEATHER_ASSETS
from grecohome_silver.dagster.weather_checks import WEATHER_CHECKS
from grecohome_silver.dagster.whoop_workouts_assets import WHOOP_WORKOUTS_ASSETS
from grecohome_silver.dagster.whoop_workouts_checks import WHOOP_WORKOUTS_CHECKS
from grecohome_silver.dagster.workout_assets import WORKOUT_ASSETS
from grecohome_silver.dagster.workout_checks import WORKOUT_CHECKS
from grecohome_silver.dagster.workout_splits_assets import WORKOUT_SPLITS_ASSETS
from grecohome_silver.dagster.workout_splits_checks import WORKOUT_SPLITS_CHECKS

defs = Definitions(
    assets=(
        SLEEP_ASSETS + GLUCOSE_ASSETS + WORKOUT_ASSETS + RECOVERY_ASSETS
        + WEATHER_ASSETS + DAILY_ASSETS + STRAIN_ASSETS + BODY_ASSETS + FITNESS_ASSETS
        + WORKOUT_SPLITS_ASSETS + WHOOP_WORKOUTS_ASSETS
    ),
    asset_checks=(
        SLEEP_CHECKS + GLUCOSE_CHECKS + WORKOUT_CHECKS + RECOVERY_CHECKS
        + WEATHER_CHECKS + DAILY_CHECKS + STRAIN_CHECKS + BODY_CHECKS + FITNESS_CHECKS
        + WORKOUT_SPLITS_CHECKS + WHOOP_WORKOUTS_CHECKS
    ),
    jobs=[
        silver_sleep_job,
        silver_glucose_job,
        silver_workouts_job,
        silver_recovery_job,
        silver_weather_job,
        silver_daily_job,
        silver_strain_job,
        silver_body_job,
        silver_fitness_job,
        silver_workout_splits_job,
        silver_whoop_workouts_job,
        silver_checks_job,
    ],
    schedules=[
        silver_sleep_daily,
        silver_glucose_daily,
        silver_workouts_daily,
        silver_recovery_daily,
        silver_weather_daily,
        silver_daily_daily,
        silver_strain_daily,
        silver_body_daily,
        silver_fitness_daily,
        silver_workout_splits_daily,
        silver_whoop_workouts_daily,
        silver_checks_schedule,
    ],
)
