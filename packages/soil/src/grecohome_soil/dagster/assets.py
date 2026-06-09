"""Bronze asset for NOAA USCRN hourly soil/temperature data.

One daily-UTC-partitioned asset. Each partition fetches the station's year file,
slices that UTC date's rows, and captures them to bronze (content-hash deduped).
Storing only the day's rows -- not the whole year file -- is what keeps a
few-times-a-day re-capture from re-storing the bulk of the file every tick.
"""

from dagster import AssetExecutionContext, asset

from grecohome_core.dagster.helpers import daily_utc_partitions
from grecohome_soil import fetch
from grecohome_soil.capture import capture_hourly
from grecohome_soil.config import settings

# Daily UTC partitions; end_offset=1 so the in-progress (current) day is a valid,
# materializable partition that the schedule re-captures intraday as rows arrive.
SOIL_PARTITIONS = daily_utc_partitions(settings.uscrn_start_date, end_offset=1)

# Optional shared concurrency pool (limit enforced on the host, if set). Low volume,
# so not critical -- present for consistency with the other subjects.
SOIL_POOL = "uscrn_api"


@asset(partitions_def=SOIL_PARTITIONS, pool=SOIL_POOL, group_name="soil")
def uscrn_bronze_hourly(context: AssetExecutionContext) -> None:
    """Capture one UTC day's USCRN rows for the configured station."""
    key = context.partition_key  # "YYYY-MM-DD"
    year = int(key[:4])
    yyyymmdd = key.replace("-", "")
    url = fetch.year_file_url(year)

    text = fetch.fetch_year_file(url)
    if text is None:
        context.add_output_metadata({"url": url, "status": "year file not found (404)"})
        return

    rows = fetch.rows_for_date(text, yyyymmdd)
    path = capture_hourly(
        rows,
        station=settings.uscrn_station,
        partition_date=key,
        year=year,
        source_url=url,
        bronze_root=settings.bronze_root,
    )
    context.add_output_metadata(
        {
            "url": url,
            "rows": len(rows),
            # False => deduped (unchanged day) or no rows yet for this date.
            "captured": path is not None,
        }
    )
