"""Tests that materializing bronze assets writes to the bronze layer.

These run the assets through Dagster's in-process executor with the Whoop API
mocked (respx) and the OAuth token patched, asserting raw payloads land under the
expected partition folder.
"""

import os
from unittest.mock import AsyncMock, patch

import httpx
import pytest
import respx
from dagster import materialize

from grecohome_whoop.config import settings
from grecohome_whoop.dagster.assets import bronze_sleep, bronze_snapshots

TOKEN_PATCH = "grecohome_whoop.auth.token_manager.TokenManager.get_valid_token"


def _files(root: str) -> list[str]:
    return [
        os.path.join(d, n).replace(os.sep, "/")
        for d, _s, names in os.walk(root)
        for n in names
    ]


@pytest.mark.integration
@respx.mock
def test_materialize_sleep_writes_partitioned_bronze(
    isolate_bronze_root, mock_whoop_sleep_response
):
    respx.get(f"{settings.whoop_api_base_url}/developer/v2/activity/sleep").mock(
        return_value=httpx.Response(200, json=mock_whoop_sleep_response)
    )
    with patch(TOKEN_PATCH, new=AsyncMock(return_value="tok")):
        result = materialize([bronze_sleep], partition_key="2025-01-05")

    assert result.success
    payloads = [
        f for f in _files(isolate_bronze_root)
        if f.endswith(".json") and not f.endswith(".meta.json")
    ]
    assert payloads
    assert all("whoop/sleep/dt=2025-01-05/" in f for f in payloads)


@pytest.mark.integration
@respx.mock
def test_materialize_snapshots_captures_profile_and_body(
    isolate_bronze_root, mock_whoop_user_profile, mock_whoop_body_measurement
):
    base = settings.whoop_api_base_url
    respx.get(f"{base}/developer/v2/user/profile/basic").mock(
        return_value=httpx.Response(200, json=mock_whoop_user_profile)
    )
    respx.get(f"{base}/developer/v2/user/measurement/body").mock(
        return_value=httpx.Response(200, json=mock_whoop_body_measurement)
    )
    with patch(TOKEN_PATCH, new=AsyncMock(return_value="tok")):
        result = materialize([bronze_snapshots])

    assert result.success
    files = _files(isolate_bronze_root)
    assert any("/whoop/profile/" in f for f in files)
    assert any("/whoop/body_measurement/" in f for f in files)
