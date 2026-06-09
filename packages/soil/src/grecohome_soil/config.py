"""Soil (NOAA USCRN) subject configuration.

Extends the shared ``BaseSubjectSettings`` with the USCRN source knobs. The source
is a public NOAA file (no auth, no secrets), so there is nothing to mount beyond the
bronze root. Fields map case-insensitively to ``USCRN_*`` env vars (Ansible-injected).
"""

from grecohome_core.config import BaseSubjectSettings, init_settings


class SoilSettings(BaseSubjectSettings):
    """Settings for the Soil/USCRN data subject (extends the shared base)."""

    # USCRN station filename stem: STATE_LOCATION_DISTANCE_DIRECTION. Combined with
    # the year into CRNH0203-{year}-{station}.txt. Single station (single-user).
    uscrn_station: str = "PA_Avondale_2_N"

    # Base URL for the hourly02 product (no trailing slash).
    uscrn_base_url: str = "https://www.ncei.noaa.gov/pub/data/uscrn/products/hourly02"

    # Trailing daily partitions the schedule re-captures each tick (catches the
    # UTC-midnight rollover and any late rows). Dedup keeps re-captures ~free.
    uscrn_lookback_days: int = 2

    # Backfill floor for the daily partition set. Early/empty partitions are
    # harmless (the asset skips the write when a date has no rows yet).
    uscrn_start_date: str = "2010-01-01"


settings: SoilSettings = init_settings(SoilSettings)  # type: ignore[assignment]
