"""Silver code-location configuration.

Silver needs read access to the bronze tree (``bronze_root``, inherited from the
shared base) and a place to write its Parquet (``silver_root``). Both roots are
swappable (local mount now, object store later) and kept strictly separate:
``silver_root`` must never live under ``bronze_root`` — :mod:`grecohome_core.silver`
enforces that at write time.
"""

from grecohome_core.config import BaseSubjectSettings, init_settings


class SilverSettings(BaseSubjectSettings):
    """Settings for the silver code location (extends the shared base)."""

    # Root directory for silver Parquet output. Required. MUST be outside
    # bronze_root (writes are refused otherwise). Mirrors the swappable-root
    # bronze convention, keeping an object-store migration open.
    silver_root: str

    # Writable dir for future silver-**check** state (schema-drift baselines, etc.),
    # kept strictly OUTSIDE silver_root so silver Parquet stays a pure projection of
    # bronze. Optional and currently unused: declared now (with its deploy mount) so
    # the forthcoming silver monitor/validation needs no config or deploy change to
    # turn on. Unset → future checks no-op. Mirrors bronze_monitor_dir exactly.
    silver_monitor_dir: str | None = None


settings: SilverSettings = init_settings(SilverSettings)  # type: ignore[assignment]
