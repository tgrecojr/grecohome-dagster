"""Tests for the plaintext-JSON token file store."""

import json

import pytest

from grecohome_core.tokens.file_store import TokenFileStore


@pytest.mark.unit
class TestTokenFileStore:
    def test_read_missing_returns_none(self, tmp_path):
        store = TokenFileStore(str(tmp_path / "nope.json"))
        assert store.read() is None

    def test_round_trip(self, tmp_path):
        store = TokenFileStore(str(tmp_path / "token.json"))
        data = {
            "access_token": "abc",
            "refresh_token": "xyz",
            "token_type": "Bearer",
            "expires_at": "2026-06-08T12:00:00+00:00",
            "scopes": ["read:sleep", "read:recovery"],
        }
        store.write_atomic(data)
        assert store.read() == data

    def test_write_creates_parent_dirs(self, tmp_path):
        path = tmp_path / "nested" / "deep" / "token.json"
        store = TokenFileStore(str(path))
        store.write_atomic({"access_token": "a"})
        assert path.exists()

    def test_atomic_rewrite_preserves_rotated_refresh_token(self, tmp_path):
        store = TokenFileStore(str(tmp_path / "token.json"))
        store.write_atomic({"access_token": "a1", "refresh_token": "r1"})
        # Simulate a refresh that rotated the refresh token.
        store.write_atomic({"access_token": "a2", "refresh_token": "r2"})
        got = store.read()
        assert got["access_token"] == "a2"
        assert got["refresh_token"] == "r2"

    def test_no_temp_files_left_behind(self, tmp_path):
        store = TokenFileStore(str(tmp_path / "token.json"))
        store.write_atomic({"access_token": "a"})
        leftover = [p.name for p in tmp_path.iterdir() if p.name.startswith(".tmp_")]
        assert leftover == []

    def test_malformed_file_raises(self, tmp_path):
        path = tmp_path / "token.json"
        path.write_text("{ not json")
        store = TokenFileStore(str(path))
        with pytest.raises(json.JSONDecodeError):
            store.read()
