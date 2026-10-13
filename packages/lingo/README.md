# grecohome-lingo

The Lingo CGM data subject for `grecohome-dagster`: a **bronze-only** Dagster code location
(`lingo_ingest`, image `grecohome-dagster-lingo`). Ported from `glucose-loader`.

Lingo has no API: glucose exports are uploaded (sporadically) from the Lingo iOS app to a Google
Drive folder. A **sensor** captures each new export once (one dynamic partition per Drive
`file_id`), reading the folder with a **read-only service account**.

- Design (sensor, capture, auth, checks, and why): [docs/intent/lingo/lingo-design.md](../../docs/intent/lingo/lingo-design.md)
- Specs: [docs/intent/lingo/lingo-specs.md](../../docs/intent/lingo/lingo-specs.md)
- Deployment and GCP service-account setup: [docs/DEPLOYMENT.md](../../docs/DEPLOYMENT.md)
