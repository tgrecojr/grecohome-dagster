"""grecohome-location: phone location-capture data subject (bronze-only Dagster code location).

Promotes the raw staging files produced by the external ``locationrelay`` Rust
service (Overland + OwnTracks POST bodies, byte-exact) into the bronze lake via the
shared core writer. The lake stays single-writer (Python) and the bronze contract
stays single-sourced in ``grecohome_core``.

``__version__`` is recorded in every bronze sidecar as ``processor_version``.
"""

__version__ = "0.1.0"
