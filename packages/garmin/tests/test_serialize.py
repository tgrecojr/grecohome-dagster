"""Tests for serialize: deterministic JSON + secret-screening."""

import pytest
from grecohome_garmin.serialize import screen_for_secrets, to_bronze_json


@pytest.mark.unit
class TestToBronzeJson:
    def test_compact_no_sort_keys_utf8(self):
        # Key order preserved (no sort_keys), compact separators, UTF-8 kept.
        out = to_bronze_json({"b": 1, "a": "é"})
        assert out == '{"b":1,"a":"é"}'.encode()

    def test_roundtrip_value(self):
        import json
        obj = {"records": [{"id": 1}], "next": None}
        assert json.loads(to_bronze_json(obj)) == obj


@pytest.mark.unit
class TestScreenForSecrets:
    def test_removes_secret_keys_and_reports_paths(self):
        obj = {"profile": {"accessToken": "x", "name": "ok"}, "password": "p"}
        screened, redacted = screen_for_secrets(obj)
        assert screened == {"profile": {"name": "ok"}}
        assert set(redacted) == {"profile.accessToken", "password"}

    def test_no_secrets_is_identity(self):
        obj = {"a": 1, "b": [{"c": 2}]}
        screened, redacted = screen_for_secrets(obj)
        assert screened == obj
        assert redacted == []

    def test_does_not_mutate_input(self):
        obj = {"token": "secret", "keep": 1}
        screen_for_secrets(obj)
        assert obj == {"token": "secret", "keep": 1}  # original untouched

    def test_indexed_paths_in_lists(self):
        obj = {"items": [{"apiKey": "k"}]}
        _, redacted = screen_for_secrets(obj)
        assert redacted == ["items[0].apiKey"]
