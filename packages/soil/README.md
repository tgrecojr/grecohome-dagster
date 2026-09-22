# grecohome-soil

NOAA USCRN soil/temperature data subject for `grecohome-dagster` — a **bronze-only** Dagster code
location. Ported from [`soildata`](https://github.com/tgrecojr/soildata).

The source is a public NOAA USCRN station file (`hourly02` product, no auth): one headerless,
whitespace-delimited file per station-year that gains one row per hour. To avoid re-storing the
whole year on every fetch, each **daily UTC partition** captures only *that day's rows* (sliced from
the year file by the `UTC_DATE` column) with content-hash dedup — a finished day stores once, and
today re-writes only when a new row appears. A schedule re-captures the trailing few days a few
times a day.

Two source quirks are handled here:

- **NOAA back-corrects the year files in place, indefinitely** — hours that first arrive as
  "no transmission" rows are filled in later (seen up to two years late) and corrupted rows are
  repaired. A day is therefore never final. A **weekly correction schedule** re-slices the trailing
  `USCRN_CORRECTION_LOOKBACK_DAYS` (default 400) partitions in **one partition-range run**: each
  year file is downloaded once per run, unchanged days dedup to nothing, and a corrected day lands
  as a new capture that silver's latest-wins dedup picks up. UI/CLI backfills are chunked the same
  way (`BackfillPolicy.multi_run`, ≤ 366 partitions per run). Corrections older than the window
  need a manual backfill of the affected partitions.
- **The `YYYY0101 0000` row lives in the previous year's file.** The Jan 1 partition reads both
  year files so that hour is not lost.

The `updates/` directory of the product is the raw real-time NOAAPort feed (never corrected) and
`snapshots/` are periodic full-archive zips; neither is used — the year files are the corrected
product.

See [docs/ARCHITECTURE.md](../../docs/ARCHITECTURE.md) and
[docs/DEPLOYMENT.md](../../docs/DEPLOYMENT.md).
