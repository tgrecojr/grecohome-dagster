"""Tests for the Garmin token-store guards.

garminconnect fails silently when its token store can't be used: every run then
does a full password login, and a burst of those gets rate-limited by Garmin.
These tests pin the checks that make a broken store loud.
"""

import json
import os
import time
from pathlib import Path

import pytest
from grecohome_garmin import auth, bootstrap
from grecohome_garmin.config import GarminSettings
from structlog.testing import capture_logs

SESSION = {"di_token": "tok-secret-111", "di_refresh_token": "ref-secret-222", "di_client_id": "c"}
INLINE = json.dumps(SESSION)


@pytest.fixture(autouse=True)
def no_backoff_sleep(monkeypatch):
    monkeypatch.setattr(auth._login_with_retry.retry, "wait", lambda *a, **k: 0)


@pytest.fixture
def mounted(monkeypatch, tmp_path):
    """Treat tmp_path as a mount point (like /secrets/garmin) and nothing else."""
    monkeypatch.setattr(auth, "_is_mount", lambda p: Path(p) == tmp_path)
    return tmp_path


def _store(root: Path) -> Path:
    store = root / "tokens"
    store.mkdir()
    return store


def _write_session(store: Path, session: dict, *, age_seconds: float = 0) -> Path:
    path = store / "garmin_tokens.json"
    path.write_text(json.dumps(session))
    if age_seconds:
        old = time.time() - age_seconds
        os.utime(path, (old, old))
    return path


def _stub_garmin(monkeypatch, session: dict, effects=(None,), save_to: Path | None = None):
    """Patch auth.Garmin with a stub whose in-memory session is ``session``."""
    calls = {"login": 0}
    outcomes = iter(effects)

    class _Inner:
        def dumps(self):
            return json.dumps(session)

    class _Stub:
        def __init__(self, **kwargs):
            self.client = _Inner()

        def login(self, tokenstore=None):
            calls["login"] += 1
            outcome = next(outcomes)
            if isinstance(outcome, Exception):
                raise outcome
            if save_to is not None:
                _write_session(save_to, session)

    monkeypatch.setattr(auth, "Garmin", _Stub)
    return calls


def _settings(tokenstore) -> GarminSettings:
    return GarminSettings(garmintokens=str(tokenstore))


def _events(logs, name):
    return [e for e in logs if e["event"] == name]


@pytest.mark.unit
class TestTokenstoreProblem:
    # @spec GARMIN-AUTH-001
    def test_usable_mounted_directory_has_no_problem(self, mounted):
        assert auth.tokenstore_problem(str(_store(mounted))) is None

    def test_inline_json(self, mounted):
        assert auth.tokenstore_problem(INLINE) == "inline_json"

    def test_symlink_is_rejected(self, mounted):
        real = _store(mounted)
        link = mounted / "link"
        link.symlink_to(real, target_is_directory=True)
        assert auth.tokenstore_problem(str(link)) == "path_rejected"

    @pytest.mark.skipif(os.geteuid() == 0, reason="root ignores directory permissions")
    def test_existing_directory_not_writable(self, mounted):
        store = _store(mounted)
        store.chmod(0o500)
        try:
            assert auth.tokenstore_problem(str(store)) == "not_writable"
        finally:
            store.chmod(0o700)

    @pytest.mark.skipif(os.geteuid() == 0, reason="root ignores directory permissions")
    def test_missing_directory_under_unwritable_parent(self, mounted):
        parent = _store(mounted)
        parent.chmod(0o500)
        try:
            assert auth.tokenstore_problem(str(parent / "tokens")) == "parent_not_writable"
        finally:
            parent.chmod(0o700)

    def test_off_volume_even_after_directory_exists(self, monkeypatch, tmp_path):
        monkeypatch.setattr(auth, "_is_mount", lambda p: False)  # e.g. /app/data/tokens
        assert auth.tokenstore_problem(str(_store(tmp_path))) == "off_volume"


@pytest.mark.unit
class TestSessionSaved:
    # @spec GARMIN-AUTH-002
    def test_missing_file_is_not_saved(self, tmp_path):
        assert not auth.session_saved(str(_store(tmp_path)), INLINE, since=time.time())

    def test_old_file_with_different_session_is_not_saved(self, tmp_path):
        store = _store(tmp_path)
        _write_session(store, {**SESSION, "di_token": "stale"}, age_seconds=3600)
        assert not auth.session_saved(str(store), INLINE, since=time.time())

    def test_old_file_with_same_session_is_saved(self, tmp_path):
        store = _store(tmp_path)
        _write_session(store, SESSION, age_seconds=3600)
        assert auth.session_saved(str(store), INLINE, since=time.time())

    def test_file_rewritten_since_login_started_is_saved(self, tmp_path):
        # A concurrent run refreshed and saved its own session: the store is live.
        store = _store(tmp_path)
        start = time.time() - 5
        _write_session(store, {**SESSION, "di_token": "other-run"})
        assert auth.session_saved(str(store), INLINE, since=start)


@pytest.mark.unit
class TestLoginGuards:
    # @spec GARMIN-AUTH-001
    def test_unusable_store_is_logged_and_login_still_happens(self, monkeypatch, tmp_path):
        monkeypatch.setattr(auth, "_is_mount", lambda p: False)
        store = _store(tmp_path)
        calls = _stub_garmin(monkeypatch, SESSION, save_to=store)
        with capture_logs() as logs:
            auth.login(_settings(store))
        (event,) = _events(logs, "garmin_tokenstore_unusable")
        assert event["log_level"] == "error"
        assert (event["reason"], event["tokenstore"]) == ("off_volume", str(store))
        assert calls["login"] == 1

    def test_inline_json_value_is_never_logged(self, monkeypatch, mounted):
        _stub_garmin(monkeypatch, SESSION)
        with capture_logs() as logs:
            auth.login(_settings(INLINE))
        logged = " ".join(str(v) for e in logs for v in e.values())
        assert _events(logs, "garmin_tokenstore_unusable")
        assert "tok-secret-111" not in logged and "ref-secret-222" not in logged

    # @spec GARMIN-AUTH-002
    def test_unsaved_session_is_logged_without_contents(self, monkeypatch, mounted):
        store = _store(mounted)
        _write_session(store, {**SESSION, "di_token": "stale"}, age_seconds=3600)
        _stub_garmin(monkeypatch, SESSION)  # logs in but never saves
        with capture_logs() as logs:
            auth.login(_settings(store))
        (event,) = _events(logs, "garmin_tokenstore_not_saved")
        assert event["log_level"] == "error" and event["tokenstore"] == str(store)
        logged = " ".join(str(v) for e in logs for v in e.values())
        assert "tok-secret-111" not in logged and "stale" not in logged

    def test_saved_session_logs_nothing(self, monkeypatch, mounted):
        store = _store(mounted)
        _stub_garmin(monkeypatch, SESSION, save_to=store)
        with capture_logs() as logs:
            auth.login(_settings(store))
        assert not _events(logs, "garmin_tokenstore_unusable")
        assert not _events(logs, "garmin_tokenstore_not_saved")

    # @spec GARMIN-AUTH-003
    def test_checks_run_once_even_when_login_is_retried(self, monkeypatch, tmp_path):
        from garminconnect.exceptions import GarminConnectConnectionError

        monkeypatch.setattr(auth, "_is_mount", lambda p: False)
        store = _store(tmp_path)
        calls = _stub_garmin(
            monkeypatch, SESSION, effects=(GarminConnectConnectionError("blip"), None)
        )
        with capture_logs() as logs:
            auth.login(_settings(store))
        assert calls["login"] == 2
        assert len(_events(logs, "garmin_tokenstore_unusable")) == 1
        assert len(_events(logs, "garmin_tokenstore_not_saved")) == 1


@pytest.mark.unit
class TestBootstrap:
    # @spec GARMIN-AUTH-005
    def test_exits_nonzero_when_session_not_saved(self, monkeypatch, mounted):
        store = _store(mounted)
        _stub_garmin(monkeypatch, SESSION)  # never saves
        monkeypatch.setattr(bootstrap, "settings", _settings(store))
        assert bootstrap.main() == 1

    def test_exits_zero_when_session_saved(self, monkeypatch, mounted):
        store = _store(mounted)
        _stub_garmin(monkeypatch, SESSION, save_to=store)
        monkeypatch.setattr(bootstrap, "settings", _settings(store))
        assert bootstrap.main() == 0


@pytest.mark.unit
class TestTokenstoreHealth:
    # @spec GARMIN-AUTH-HEALTH-001
    def test_valid_session_file_passes(self, mounted):
        store = _store(mounted)
        _write_session(store, SESSION)
        passed, _ = auth.tokenstore_health(str(store))
        assert passed

    @pytest.mark.parametrize(
        ("content", "reason"),
        [(None, "missing"), ("not json", "not_json"), ("{}", "missing_session_fields")],
    )
    def test_bad_token_file_fails(self, mounted, content, reason):
        store = _store(mounted)
        if content is not None:
            (store / "garmin_tokens.json").write_text(content)
        passed, metadata = auth.tokenstore_health(str(store))
        assert not passed and metadata["reason"] == reason

    def test_unusable_store_fails_even_with_a_good_file(self, monkeypatch, tmp_path):
        monkeypatch.setattr(auth, "_is_mount", lambda p: False)
        store = _store(tmp_path)
        _write_session(store, SESSION)
        passed, metadata = auth.tokenstore_health(str(store))
        assert not passed and metadata["reason"] == "off_volume"

    def test_check_fails_at_error_and_logs(self, monkeypatch, mounted):
        from dagster import AssetCheckSeverity
        from grecohome_garmin.dagster import checks

        monkeypatch.setattr(checks.settings, "garmintokens", str(_store(mounted)))
        with capture_logs() as logs:
            result = checks.garmin_tokenstore_health()
        assert result.passed is False and result.severity == AssetCheckSeverity.ERROR
        assert _events(logs, "garmin_tokenstore_unhealthy")

    def test_check_is_registered_on_garmin_bronze_sleep(self):
        from grecohome_garmin.dagster.definitions import defs

        keys = {(str(k.asset_key), k.name) for k in defs.resolve_asset_graph().asset_check_keys}
        assert ("AssetKey(['garmin_bronze_sleep'])", "garmin_tokenstore_health") in keys

    # @spec GARMIN-AUTH-HEALTH-002
    def test_reports_path_and_reason_never_contents(self, mounted):
        store = _store(mounted)
        _write_session(store, {"di_token": "tok-secret-111"})  # missing fields -> fails
        _, metadata = auth.tokenstore_health(str(store))
        assert "tok-secret-111" not in json.dumps(metadata)
        _, inline_md = auth.tokenstore_health(INLINE)
        assert "tok-secret-111" not in json.dumps(inline_md)
        assert inline_md["reason"] == "inline_json"
