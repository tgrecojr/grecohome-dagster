"""Tests for GarminSettings (selection/exclude logic, defaults)."""

import pytest
from grecohome_garmin.config import GarminSettings


@pytest.mark.unit
class TestGarminSettings:
    def test_imports_and_inherits_bronze_root(self):
        # BRONZE_ROOT is provided by the test env (pyproject pytest-env).
        s = GarminSettings()
        assert s.bronze_root  # required, inherited from BaseSubjectSettings

    def test_fetch_exclude_defaults_empty(self):
        # Locked decision: capture the full surface by default.
        assert GarminSettings().fetch_exclude == ""
        assert GarminSettings().fetch_exclude_set == set()

    def test_is_selected_empty_selection_means_all(self):
        s = GarminSettings()
        assert s.is_selected("sleep") is True
        assert s.is_selected("hrv") is True

    def test_exclude_wins_over_selection(self, monkeypatch):
        monkeypatch.setenv("FETCH_SELECTION", "sleep,hrv")
        monkeypatch.setenv("FETCH_EXCLUDE", "hrv")
        s = GarminSettings()
        assert s.is_selected("sleep") is True
        assert s.is_selected("hrv") is False  # excluded
        assert s.is_selected("stress") is False  # not in selection

    def test_selection_subset(self, monkeypatch):
        monkeypatch.setenv("FETCH_SELECTION", "sleep")
        monkeypatch.delenv("FETCH_EXCLUDE", raising=False)
        s = GarminSettings()
        assert s.is_selected("sleep") is True
        assert s.is_selected("stress") is False
