"""Tests for the Garmin endpoint catalog -- especially the safety invariants."""

import pytest
from garminconnect import Garmin
from grecohome_garmin import catalog


@pytest.mark.unit
class TestCatalogSafety:
    def test_no_forbidden_methods_in_catalog(self):
        # The critical invariant: no mutating/auth/plumbing method is ever callable.
        assert catalog.detect_forbidden_in_catalog() == []

    def test_known_redundant_disjoint_from_catalog(self):
        # A redundant getter must never also be in the allowlist.
        assert catalog._KNOWN_REDUNDANT.isdisjoint(catalog.catalog_method_names())

    def test_every_catalog_method_is_a_reader(self):
        for method in catalog.catalog_method_names():
            assert method.startswith(("get_", "download_"))


@pytest.mark.unit
class TestCatalogShape:
    def test_lookup_and_collections(self):
        ep = catalog.get("sleep")
        assert ep is not None
        assert ep.method == "get_sleep_data"
        assert ep.kind == catalog.KIND_DAILY
        assert catalog.get("does_not_exist") is None

    def test_by_kind(self):
        daily = catalog.by_kind(catalog.KIND_DAILY)
        assert all(ep.kind == catalog.KIND_DAILY for ep in daily)
        assert any(ep.name == "sleep" for ep in daily)

    def test_per_activity_and_downloads_present(self):
        names = {n for n, _ in catalog.PER_ACTIVITY}
        assert {"activity_summary", "activity_details"} <= names
        assert ("activity_fit", "ORIGINAL") in catalog.PER_ACTIVITY_DOWNLOAD


@pytest.mark.unit
class TestDriftDetection:
    def test_drift_returns_sorted_list_of_readers(self):
        drift = catalog.detect_catalog_drift(Garmin)
        assert isinstance(drift, list)
        assert drift == sorted(drift)
        # Anything reported must be a readable getter, never a mutating method.
        for name in drift:
            assert name.startswith(("get_", "download_"))
            assert not any(name.startswith(p) for p in catalog._DANGEROUS_PREFIXES)
