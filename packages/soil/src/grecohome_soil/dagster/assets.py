"""Bronze asset for NOAA USCRN hourly soil/temperature data.

One daily-UTC-partitioned asset. Each partition slices that UTC date's rows out of
the station's year file(s) and captures them to bronze (content-hash deduped).
Storing only the day's rows -- not the whole year file -- is what keeps a
few-times-a-day re-capture from re-storing the bulk of the file every tick.

The asset is **partition-range aware**: a run may cover one partition (the 6-hourly
schedule) or a contiguous range (the weekly correction run and UI backfills, see
``backfill_policy``). Within a run each year file is downloaded once and every
partition in the range is sliced from it, so re-capturing a 400-day window costs
two HTTP GETs, not 400.
"""

from dagster import AssetExecutionContext, BackfillPolicy, asset

from grecohome_core.dagster.helpers import daily_utc_partitions
from grecohome_soil.capture import capture_hourly
from grecohome_soil.config import settings
from grecohome_soil.fetch import YearFiles, rows_for_partition, year_file_url, years_for_date

# Daily UTC partitions; end_offset=1 so the in-progress (current) day is a valid,
# materializable partition that the schedule re-captures intraday as rows arrive.
SOIL_PARTITIONS = daily_utc_partitions(settings.uscrn_start_date, end_offset=1)

# Optional shared concurrency pool (limit enforced on the host, if set). Low volume,
# so not critical -- present for consistency with the other subjects.
SOIL_POOL = "uscrn_api"

# UI/CLI backfills are chunked into runs of up to a year of partitions, so each run
# fetches (about) one year file. Single-partition runs are unaffected.
MAX_PARTITIONS_PER_RUN = 366


@asset(
    partitions_def=SOIL_PARTITIONS,
    pool=SOIL_POOL,
    group_name="soil",
    backfill_policy=BackfillPolicy.multi_run(max_partitions_per_run=MAX_PARTITIONS_PER_RUN),
)
def uscrn_bronze_hourly(context: AssetExecutionContext) -> None:
    """Capture each UTC day's USCRN rows (one partition or a contiguous range)."""
    keys = list(context.partition_keys)  # ["YYYY-MM-DD", ...]; one key for a normal run
    files = YearFiles()
    rows_total = 0
    captured = 0
    for key in keys:
        yyyymmdd = key.replace("-", "")
        rows = rows_for_partition(files, yyyymmdd)
        rows_total += len(rows)
        path = capture_hourly(
            rows,
            station=settings.uscrn_station,
            partition_date=key,
            year_files=years_for_date(yyyymmdd),
            source_url=year_file_url(int(key[:4])),
            bronze_root=settings.bronze_root,
        )
        # None => deduped (unchanged day) or no rows yet for this date.
        captured += path is not None
    context.add_output_metadata(
        {
            "partitions": len(keys),
            "first_partition": keys[0],
            "last_partition": keys[-1],
            "rows": rows_total,
            "captured": captured,
            "year_files": files.status,
        }
    )
