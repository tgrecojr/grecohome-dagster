# grecohome-soil

NOAA USCRN soil/temperature data subject for `grecohome-dagster` — a **bronze-only** Dagster code
location. Ported from [`soildata`](https://github.com/tgrecojr/soildata).

The source is a public NOAA USCRN station file (`hourly02` product, no auth): one headerless,
whitespace-delimited file per station-year that gains one row per hour. To avoid re-storing the
whole year on every fetch, each **daily UTC partition** captures only *that day's rows* (sliced from
the year file by the `UTC_DATE` column) with content-hash dedup — a finished day stores once, and
today re-writes only when a new row appears. A schedule re-captures the trailing few days a few
times a day.

See [docs/ARCHITECTURE.md](../../docs/ARCHITECTURE.md) and
[docs/DEPLOYMENT.md](../../docs/DEPLOYMENT.md).
