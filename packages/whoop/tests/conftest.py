"""Fixtures for grecohome-whoop tests (no database, no encryption)."""

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from grecohome_whoop.config import settings


@pytest.fixture(autouse=True)
def isolate_bronze_root(tmp_path, monkeypatch):
    """Point bronze capture at a per-test temp dir so tests never pollute disk."""
    monkeypatch.setattr(settings, "bronze_root", str(tmp_path / "bronze"))
    return str(tmp_path / "bronze")


@pytest.fixture
def mock_whoop_sleep_response() -> dict:
    return {
        "records": [
            {
                "id": str(uuid4()),
                "cycle_id": 1545424244,
                "start": "2025-12-18T00:00:00.000Z",
                "end": "2025-12-18T08:00:00.000Z",
                "timezone_offset": "-05:00",
                "nap": False,
                "score_state": "SCORED",
                "score": {"sleep_performance_percentage": 85.5},
            }
        ],
        "next_token": None,
    }


@pytest.fixture
def mock_whoop_recovery_response() -> dict:
    return {
        "records": [
            {
                "id": str(uuid4()),
                "cycle_id": 1545424244,
                "sleep_id": str(uuid4()),
                "created_at": "2025-12-18T08:00:00.000Z",
                "score_state": "SCORED",
                "score": {"recovery_score": 75.0, "resting_heart_rate": 55},
            }
        ],
        "next_token": None,
    }


@pytest.fixture
def mock_whoop_workout_response() -> dict:
    return {
        "records": [
            {
                "id": str(uuid4()),
                "start": "2025-12-18T10:00:00.000Z",
                "end": "2025-12-18T11:00:00.000Z",
                "sport_id": 1,
                "sport_name": "Running",
                "score_state": "SCORED",
                "score": {"strain": 12.5},
            }
        ],
        "next_token": None,
    }


@pytest.fixture
def mock_whoop_cycle_response() -> dict:
    return {
        "records": [
            {
                "id": 1545424244,
                "start": "2025-12-17T00:00:00.000Z",
                "end": "2025-12-18T00:00:00.000Z",
                "score_state": "SCORED",
                "score": {"strain": 14.5},
            }
        ],
        "next_token": None,
    }


@pytest.fixture
def mock_whoop_body_measurement() -> dict:
    return {"height_meter": 1.8288, "weight_kilogram": 90.7185, "max_heart_rate": 190}


@pytest.fixture
def mock_whoop_user_profile() -> dict:
    return {
        "user_id": str(uuid4()),
        "first_name": "Test",
        "last_name": "User",
        "email": "test@example.com",
    }


@pytest.fixture
def date_range():
    end = datetime.now(UTC)
    return end - timedelta(days=30), end
