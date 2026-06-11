"""Dagster Definitions for the silver code location (the gRPC server target).

Served by ``dagster code-server start -m grecohome_silver.dagster.definitions`` and
registered with the host daemon/webserver via the host's ``workspace.yaml``.
"""

from dagster import Definitions

from grecohome_silver.dagster.assets import SLEEP_ASSETS
from grecohome_silver.dagster.checks import SLEEP_CHECKS
from grecohome_silver.dagster.glucose_assets import GLUCOSE_ASSETS
from grecohome_silver.dagster.glucose_checks import GLUCOSE_CHECKS
from grecohome_silver.dagster.recovery_assets import RECOVERY_ASSETS
from grecohome_silver.dagster.recovery_checks import RECOVERY_CHECKS
from grecohome_silver.dagster.schedules import (
    silver_checks_job,
    silver_checks_schedule,
    silver_glucose_daily,
    silver_glucose_job,
    silver_recovery_daily,
    silver_recovery_job,
    silver_sleep_daily,
    silver_sleep_job,
    silver_workouts_daily,
    silver_workouts_job,
)
from grecohome_silver.dagster.workout_assets import WORKOUT_ASSETS
from grecohome_silver.dagster.workout_checks import WORKOUT_CHECKS

defs = Definitions(
    assets=SLEEP_ASSETS + GLUCOSE_ASSETS + WORKOUT_ASSETS + RECOVERY_ASSETS,
    asset_checks=SLEEP_CHECKS + GLUCOSE_CHECKS + WORKOUT_CHECKS + RECOVERY_CHECKS,
    jobs=[
        silver_sleep_job,
        silver_glucose_job,
        silver_workouts_job,
        silver_recovery_job,
        silver_checks_job,
    ],
    schedules=[
        silver_sleep_daily,
        silver_glucose_daily,
        silver_workouts_daily,
        silver_recovery_daily,
        silver_checks_schedule,
    ],
)
