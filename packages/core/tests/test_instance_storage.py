"""Guard the code-location images against the host instance's Postgres storage config.

Every code-location server builds the Dagster instance from the host's mounted
``dagster.yaml`` at startup, so the SQLAlchemy driver that config names must be importable
in our images. SQLAlchemy 2.1 switched the default driver for a bare ``postgresql://`` URL
from psycopg2 to psycopg (v3), which ``dagster-postgres`` does not ship. Leaving the scheme
at its default took down every code location on 2026-09-29. The host now pins the driver
explicitly. These tests fail CI if our images can't load that driver.
"""

import pytest
from dagster._config import validate_config
from dagster._core.storage.sql import create_engine
from dagster_postgres import DagsterPostgresStorage
from dagster_postgres.utils import pg_url_from_config

# Mirrors `storage.postgres.postgres_db` in greco-home-builder
# grecohome/roles/docker/templates/dagster/dagster.yaml.j2 (credentials are placeholders;
# the host resolves them from DAGSTER_POSTGRES_* env vars). Keep `scheme` in sync.
HOST_POSTGRES_DB = {
    "username": "dagster",
    "password": "placeholder",
    "hostname": "postgres",
    "db_name": "dagster",
    "port": 5432,
    "scheme": "postgresql+psycopg2",
}


@pytest.mark.unit
class TestHostInstanceStorageConfig:
    def test_storage_config_is_valid(self):
        result = validate_config(
            DagsterPostgresStorage.config_type(), {"postgres_db": HOST_POSTGRES_DB}
        )
        assert result.success, result.errors

    def test_configured_driver_is_importable(self):
        # create_engine resolves (imports) the DBAPI driver without connecting.
        engine = create_engine(pg_url_from_config({"postgres_db": HOST_POSTGRES_DB}))
        assert engine.dialect.driver == "psycopg2"
