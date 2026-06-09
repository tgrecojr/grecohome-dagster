"""Fetch + date-slice the NOAA USCRN hourly station file.

The source is one public, headerless, whitespace-delimited file per station-year
(no auth) that gains one row per hour. We fetch the whole file but keep only the
rows for a given UTC date (field 2 = ``UTC_DATE``, ``YYYYMMDD``) -- so a daily
partition's bronze payload is just that day's rows, never the whole year.
"""

import httpx

from grecohome_core.logging_config import get_logger
from grecohome_soil import __version__
from grecohome_soil.config import settings

log = get_logger(__name__)

# 0-based field index of UTC_DATE (YYYYMMDD) in a CRNH0203 row. Per the product's
# HEADERS.txt the order is: WBANNO, UTC_DATE, UTC_TIME, ... so UTC_DATE is field 2
# (1-based) -> index 1.
_UTC_DATE_FIELD = 1

_USER_AGENT = f"grecohome-soil/{__version__} (+https://github.com/tgrecojr/grecohome-dagster)"


def year_file_url(year: int, station: str | None = None, base_url: str | None = None) -> str:
    """Build the CRNH0203 year-file URL for a station (defaults from settings)."""
    station = station or settings.uscrn_station
    base = (base_url or settings.uscrn_base_url).rstrip("/")
    return f"{base}/{year}/CRNH0203-{year}-{station}.txt"


def fetch_year_file(url: str, *, timeout: float = 30.0) -> str | None:
    """GET the year file as text.

    Returns ``None`` on 404 (the file isn't present yet -- e.g. a backfilled year
    with no station data, or very early in a new year) so wide backfills stay
    robust. Other HTTP errors raise.
    """
    with httpx.Client(timeout=timeout, headers={"User-Agent": _USER_AGENT}) as client:
        resp = client.get(url)
    if resp.status_code == 404:
        log.warning("uscrn year file not found", url=url)
        return None
    resp.raise_for_status()
    return resp.text


def rows_for_date(text: str, yyyymmdd: str) -> list[str]:
    """Return the original lines whose UTC_DATE (field 2) equals ``yyyymmdd``.

    Pure selection: the returned lines are byte-faithful to the source (no parsing
    or reformatting of values). Blank and too-short lines are skipped.
    """
    matched: list[str] = []
    for line in text.splitlines():
        if not line.strip():
            continue
        fields = line.split()
        if len(fields) > _UTC_DATE_FIELD and fields[_UTC_DATE_FIELD] == yyyymmdd:
            matched.append(line)
    return matched
