"""Tests for the USCRN fetch + date-slice helpers."""

import httpx
import pytest
import respx
from grecohome_soil import fetch

# Two UTC dates, plus a blank line and a too-short line that must be ignored.
SAMPLE = "\n".join(
    [
        "03761 20260608 2300 20260608 1900 2.5 2.4 2.6 2.3 0.0",
        "03761 20260609 0000 20260608 2000 2.0 1.9 2.1 1.8 0.0",
        "03761 20260609 0100 20260608 2100 1.8 1.7 1.9 1.6 0.0",
        "",
        "short line",
    ]
)


@pytest.mark.unit
class TestYearFileUrl:
    def test_defaults_from_settings(self):
        url = fetch.year_file_url(2026)
        assert url == (
            "https://www.ncei.noaa.gov/pub/data/uscrn/products/hourly02/"
            "2026/CRNH0203-2026-PA_Avondale_2_N.txt"
        )

    def test_overrides_and_strips_trailing_slash(self):
        url = fetch.year_file_url(2024, station="CA_Bodega_6_WSW", base_url="https://x/h02/")
        assert url == "https://x/h02/2024/CRNH0203-2024-CA_Bodega_6_WSW.txt"


@pytest.mark.unit
class TestRowsForDate:
    def test_selects_only_matching_date(self):
        rows = fetch.rows_for_date(SAMPLE, "20260609")
        assert len(rows) == 2
        assert all(r.split()[1] == "20260609" for r in rows)

    def test_returns_lines_verbatim(self):
        rows = fetch.rows_for_date(SAMPLE, "20260608")
        assert rows == ["03761 20260608 2300 20260608 1900 2.5 2.4 2.6 2.3 0.0"]

    def test_no_match_is_empty(self):
        assert fetch.rows_for_date(SAMPLE, "20250101") == []


@pytest.mark.unit
class TestFetchYearFile:
    @respx.mock
    def test_ok(self):
        url = "https://x/h02/2026/CRNH0203-2026-PA_Avondale_2_N.txt"
        respx.get(url).mock(return_value=httpx.Response(200, text=SAMPLE))
        assert fetch.fetch_year_file(url) == SAMPLE

    @respx.mock
    def test_404_returns_none(self):
        url = "https://x/h02/2026/CRNH0203-2026-Missing_Station.txt"
        respx.get(url).mock(return_value=httpx.Response(404))
        assert fetch.fetch_year_file(url) is None

    @respx.mock
    def test_5xx_raises(self):
        url = "https://x/h02/2026/CRNH0203-2026-PA_Avondale_2_N.txt"
        respx.get(url).mock(return_value=httpx.Response(503))
        with pytest.raises(httpx.HTTPStatusError):
            fetch.fetch_year_file(url)


@pytest.mark.unit
class TestYearsForDate:
    def test_ordinary_date_is_its_own_year(self):
        assert fetch.years_for_date("20260609") == [2026]
        assert fetch.years_for_date("20261231") == [2026]

    def test_jan_1_also_reads_the_prior_year_file_first(self):
        assert fetch.years_for_date("20260101") == [2025, 2026]


@pytest.mark.unit
class TestYearFilesAndRowsForPartition:
    @respx.mock
    def test_fetches_each_year_once_and_caches_404(self):
        r25 = respx.get(fetch.year_file_url(2025)).mock(return_value=httpx.Response(404))
        r26 = respx.get(fetch.year_file_url(2026)).mock(
            return_value=httpx.Response(200, text=SAMPLE)
        )
        files = fetch.YearFiles()
        assert files.get(2026) == SAMPLE
        assert files.get(2026) == SAMPLE
        assert files.get(2025) is None
        assert files.get(2025) is None
        assert r26.call_count == 1 and r25.call_count == 1
        assert files.status == {"2026": "ok", "2025": "not found (404)"}

    @respx.mock
    def test_rows_for_partition_spans_the_year_boundary(self):
        respx.get(fetch.year_file_url(2025)).mock(
            return_value=httpx.Response(
                200, text="03761 20260101 0000 20251231 1900 0.5 0.5 0.5 0.5 0.0"
            )
        )
        respx.get(fetch.year_file_url(2026)).mock(
            return_value=httpx.Response(
                200, text="03761 20260101 0100 20251231 2000 0.4 0.4 0.4 0.4 0.0"
            )
        )
        rows = fetch.rows_for_partition(fetch.YearFiles(), "20260101")
        assert [r.split()[2] for r in rows] == ["0000", "0100"]
