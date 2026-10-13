# grecohome-soil

NOAA USCRN soil/temperature data subject for `grecohome-dagster`: a **bronze-only** Dagster code
location (`soil_ingest`). Ported from [`soildata`](https://github.com/tgrecojr/soildata).

It captures one station's public `hourly02` year file (no auth) as **one UTC day's rows per daily
partition**, re-captured every 6 hours, plus a **weekly 400-day correction sweep** that picks up
NOAA's late in-place corrections of the year files.

- Design (capture, Jan 1 handling, schedules, checks, and why): [docs/intent/soil/soil-design.md](../../docs/intent/soil/soil-design.md)
- Specs: [docs/intent/soil/soil-specs.md](../../docs/intent/soil/soil-specs.md)
- Deployment: [docs/DEPLOYMENT.md](../../docs/DEPLOYMENT.md)
