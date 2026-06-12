"""Gold code-location configuration.

Gold reads silver Parquet (``silver_root``) and writes marts (``gold_root``). It does
not touch bronze, so it does **not** extend ``BaseSubjectSettings`` (which requires a
bronze root); it carries only what gold needs. Both roots are swappable and kept
separate: ``gold_root`` must never live under ``silver_root`` (writes are refused).
"""

from pydantic_settings import BaseSettings, SettingsConfigDict

from grecohome_core.config import init_settings


class GoldSettings(BaseSettings):
    """Settings for the gold code location."""

    # Root of the silver Parquet to read. Required (mount read-only in deployment).
    silver_root: str

    # Root for gold mart output. Required. MUST be outside silver_root (writes are
    # refused otherwise). Mirrors the swappable-root convention of bronze/silver.
    gold_root: str

    # Writable dir for future gold-check state, kept OUTSIDE gold_root. Optional and
    # currently unused: declared now (with its deploy mount) so a future gold monitor
    # needs no config/deploy change. Mirrors bronze_monitor_dir / silver_monitor_dir.
    gold_monitor_dir: str | None = None

    log_level: str = "INFO"
    environment: str = "development"

    model_config = SettingsConfigDict(case_sensitive=False, extra="ignore")


settings: GoldSettings = init_settings(GoldSettings)  # type: ignore[assignment]
