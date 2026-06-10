"""Shared configuration base for data-subject pipelines.

``BaseSubjectSettings`` holds the settings every subject needs (the bronze root,
log level, environment). Each subject subclasses it to add its own fields.
:func:`init_settings` constructs a settings class with a friendly, actionable
error message when required environment variables are missing.
"""

import sys

from dotenv import find_dotenv, load_dotenv
from pydantic import ValidationError
from pydantic_settings import BaseSettings, SettingsConfigDict

# Load a local .env if one exists (searched upward from the cwd). In production
# the environment is injected directly (Ansible / secrets manager) and no .env is
# present. ``override=False`` means real env vars (and pytest-env) always win.
load_dotenv(find_dotenv(usecwd=True), override=False)


class BaseSubjectSettings(BaseSettings):
    """Settings common to every data subject."""

    # Root directory for bronze raw capture. Required -- bronze is always on.
    bronze_root: str

    # Writable directory for bronze-check *state* (schema-drift baselines), kept
    # strictly OUTSIDE bronze_root so raw capture stays immutable. Optional: when
    # unset, the schema-drift check no-ops (records nothing, never fails) rather
    # than writing a baseline. Mount this in deployment (see docs/DEPLOYMENT.md).
    bronze_monitor_dir: str | None = None

    log_level: str = "INFO"
    environment: str = "development"

    model_config = SettingsConfigDict(case_sensitive=False, extra="ignore")


def init_settings(settings_cls: type[BaseSettings]) -> BaseSettings:
    """Instantiate ``settings_cls``, exiting with a clear message if config is invalid.

    Args:
        settings_cls: A ``BaseSettings`` subclass to construct from the environment.

    Returns:
        The constructed settings instance.

    Raises:
        SystemExit: If required environment variables are missing or invalid.
    """
    try:
        return settings_cls()
    except ValidationError as e:
        missing_fields = []
        invalid_fields = []
        for error in e.errors():
            field_name = error["loc"][0] if error["loc"] else "unknown"
            if error["type"] == "missing":
                missing_fields.append(str(field_name).upper())
            else:
                invalid_fields.append(
                    {"field": str(field_name).upper(), "error": error["msg"]}
                )

        lines = [
            "\n" + "=" * 70,
            "CONFIGURATION ERROR: Missing or invalid environment variables",
            "=" * 70,
        ]
        if missing_fields:
            lines.append("\nMissing required environment variables:")
            lines.extend(f"  - {f}" for f in missing_fields)
        if invalid_fields:
            lines.append("\nInvalid environment variables:")
            lines.extend(f"  - {i['field']}: {i['error']}" for i in invalid_fields)
        lines.extend(
            [
                "\nSet these in your .env file (local dev) or inject them in the",
                "deployment environment (Ansible / secrets manager).",
                "See .env.example / docs/ENV_TEMPLATE.md for the full list.",
                "=" * 70 + "\n",
            ]
        )
        print("\n".join(lines), file=sys.stderr)
        sys.exit(1)
