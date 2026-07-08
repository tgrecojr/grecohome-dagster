"""grecohome-geocode: Photon reverse-geocode cache subject (bronze-only Dagster code location).

Enriches the ``location`` bronze streams with place context. The internet-facing
sources are unchanged; this subject reads the location bronze points, snaps each to a
~11 m grid cell, and captures the self-hosted **Photon** ``/reverse`` response for any
new cell to a bronze *cache* (append-only, immutable, content-hash deduped). The cache
is the source of truth for reverse lookups; ``silver_location`` joins points to it with
a pure offline DuckDB read (no network at transform time).

``__version__`` is recorded in every bronze sidecar as ``processor_version``.
"""

__version__ = "0.1.0"
