"""The geocode run: discover new cells → look each up on Photon → cache to bronze.

Thin orchestration over :mod:`discover`, :mod:`fetch`, and :mod:`capture`. One Photon
call per new cell, queried at the cell centre; the raw response is cached to bronze and
the cell becomes "already cached" for every later run. A per-run cap bounds a first,
wide-window backfill; an HTTP/transport failure on one cell is logged and skipped (that
cell is retried next run) so one bad lookup never fails the whole materialization.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

import httpx

from grecohome_core.logging_config import get_logger
from grecohome_geocode import fetch
from grecohome_geocode.capture import capture_reverse
from grecohome_geocode.cells import cell_center
from grecohome_geocode.discover import new_cells

log = get_logger(__name__)


@dataclass
class GeocodeReport:
    """What one geocode run did (surfaced as Dagster run metadata)."""

    new_cells: int = 0
    looked_up: int = 0
    captured: int = 0
    failed: int = 0
    capped: bool = False


def geocode_cells(
    *,
    bronze_root: str,
    photon_base_url: str,
    scan_days: int,
    max_lookups: int,
    timeout: float,
    language: str,
    radius_km: float | None,
    now: datetime | None = None,
) -> GeocodeReport:
    """Look up and cache every not-yet-cached cell observed in the trailing window."""
    now = now or datetime.now(UTC)
    dt = now.strftime("%Y-%m-%d")
    todo = new_cells(bronze_root, scan_days=scan_days, now=now)

    report = GeocodeReport(new_cells=len(todo))
    if len(todo) > max_lookups:
        report.capped = True
        log.warning(
            "geocode lookups capped for this run; remainder picked up next run",
            new_cells=len(todo),
            cap=max_lookups,
        )
        todo = todo[:max_lookups]

    for lat_e4, lon_e4 in todo:
        query_lat, query_lon = cell_center(lat_e4), cell_center(lon_e4)
        report.looked_up += 1
        try:
            raw = fetch.reverse_geocode(
                query_lat,
                query_lon,
                base_url=photon_base_url,
                timeout=timeout,
                language=language,
                radius_km=radius_km,
            )
        except (httpx.HTTPError, OSError) as e:
            report.failed += 1
            log.warning(
                "photon reverse lookup failed; cell left un-cached (retried next run)",
                lat_e4=lat_e4,
                lon_e4=lon_e4,
                error=str(e),
            )
            continue
        path = capture_reverse(
            raw,
            lat_e4=lat_e4,
            lon_e4=lon_e4,
            query_lat=query_lat,
            query_lon=query_lon,
            radius_km=radius_km,
            language=language,
            dt=dt,
            bronze_root=bronze_root,
        )
        if path is None:
            report.failed += 1
            log.warning("bronze capture returned None", lat_e4=lat_e4, lon_e4=lon_e4)
            continue
        report.captured += 1

    return report
