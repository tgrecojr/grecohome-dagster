"""Fetch + date-slice the NOAA USCRN hourly station file.

The source is one public, headerless, whitespace-delimited file per station-year
(no auth) that gains one row per hour. We fetch the whole file but keep only the
rows for a given UTC date (field 2 = ``UTC_DATE``, ``YYYYMMDD``) -- so a daily
partition's bronze payload is just that day's rows, never the whole year.

Two source quirks shape the helpers here:

* **NOAA rewrites the year files in place, indefinitely.** Hours that arrive as
  "no transmission" rows (``CRX_VN = -9.000``, every value a sentinel) are filled
  in later -- we have seen gaps two years old get populated -- and corrupted rows
  are repaired. So a day is never truly final; the correction schedule re-slices a
  long trailing window (see ``dagster/schedules.py``).
* **The ``YYYY0101 0000`` row lives in the *previous* year's file** (an hourly row is
  stamped with the hour it *ends*, and the year file is cut on the LST date of the
  observation period). :func:`years_for_date` returns both files for Jan 1 so that
  hour is not lost.
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


def years_for_date(yyyymmdd: str) -> list[int]:
    """The year files that can hold rows stamped ``yyyymmdd`` (chronological order).

    Every date's rows live in its own year file, except the ``0000`` hour of Jan 1,
    which is the last row of the *previous* year's file. For Jan 1 the prior year is
    listed first so the assembled day reads ``0000, 0100, ...``.
    """
    year = int(yyyymmdd[:4])
    if yyyymmdd[4:] == "0101":
        return [year - 1, year]
    return [year]


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


class YearFiles:
    """Lazily fetched, per-run cache of year files keyed by year.

    A run that covers many partitions (the weekly correction run, or a backfill
    chunk) needs each year file once, not once per day. ``None`` is cached for a
    404 so a missing year is not re-requested.
    """

    def __init__(self) -> None:
        self._text: dict[int, str | None] = {}

    def get(self, year: int) -> str | None:
        if year not in self._text:
            self._text[year] = fetch_year_file(year_file_url(year))
        return self._text[year]

    @property
    def status(self) -> dict[str, str]:
        """``{"2026": "ok" | "not found (404)"}`` for every year fetched so far."""
        return {
            str(y): ("ok" if t is not None else "not found (404)") for y, t in self._text.items()
        }


def rows_for_partition(files: YearFiles, yyyymmdd: str) -> list[str]:
    """All rows stamped ``yyyymmdd``, drawn from every year file that can hold them."""
    rows: list[str] = []
    for year in years_for_date(yyyymmdd):
        text = files.get(year)
        if text is not None:
            rows.extend(rows_for_date(text, yyyymmdd))
    return rows
