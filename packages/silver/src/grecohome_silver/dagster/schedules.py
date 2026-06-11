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
from grecohome_silver.dagster.assets import ALL_ASSETS
from grecohome_silver.dagster.checks import ALL_CHECKS

# Daily rebuild of the three sleep assets (source intermediates + unified table).
silver_sleep_job = define_asset_job("silver_sleep_job", selection=ALL_ASSETS)

silver_sleep_daily = ScheduleDefinition(
    name="silver_sleep_daily",
    job=silver_sleep_job,
    cron_schedule="0 6 * * *",  # 06:00 UTC — after the day's bronze sleep has landed
    execution_timezone="UTC",
)

# Checks-only job + a daily schedule (the table only rebuilds daily, so hourly
# checks would add nothing). Catches a silver asset that stops materializing.
silver_checks_job = build_bronze_checks_job(ALL_CHECKS, name="silver_checks_job")
silver_checks_schedule = build_bronze_checks_schedule(
    silver_checks_job, name="silver_checks_daily", cron_schedule="0 7 * * *"
)
