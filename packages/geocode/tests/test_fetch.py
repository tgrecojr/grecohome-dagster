"""Tests for the Photon /reverse fetch helper."""

import httpx
import pytest
import respx

from grecohome_geocode import fetch

pytestmark = pytest.mark.unit

BASE = "http://photon:2322"
REVERSE = "http://photon:2322/reverse"

FC = (
    b'{"type":"FeatureCollection","features":[{"type":"Feature",'
    b'"geometry":{"type":"Point","coordinates":[-75.1,39.8]},'
    b'"properties":{"name":"Somewhere","city":"Avondale","state":"Pennsylvania",'
    b'"country":"United States","osm_key":"place","osm_value":"house"}}]}'
)


def _ok():
    return httpx.Response(200, content=FC)


class TestReverseUrl:
    def test_appends_reverse(self):
        assert fetch.reverse_url(BASE) == REVERSE

    def test_strips_trailing_slash(self):
        assert fetch.reverse_url(BASE + "/") == REVERSE


class TestReverseGeocode:
    @respx.mock
    def test_ok_returns_raw_bytes(self):
        route = respx.get(REVERSE).mock(return_value=_ok())
        out = fetch.reverse_geocode(39.8, -75.1, base_url=BASE, language="en", radius_km=0.05)
        assert out == FC
        # Query params carried through.
        req = route.calls.last.request
        assert req.url.params["lat"] == "39.8"
        assert req.url.params["lon"] == "-75.1"
        assert req.url.params["lang"] == "en"
        assert req.url.params["radius"] == "0.05"

    @respx.mock
    def test_omits_radius_when_none(self):
        route = respx.get(REVERSE).mock(return_value=_ok())
        fetch.reverse_geocode(39.8, -75.1, base_url=BASE, radius_km=None)
        assert "radius" not in route.calls.last.request.url.params

    @respx.mock
    def test_5xx_raises(self):
        respx.get(REVERSE).mock(return_value=httpx.Response(500))
        with pytest.raises(httpx.HTTPStatusError):
            fetch.reverse_geocode(39.8, -75.1, base_url=BASE)

    @respx.mock
    def test_retries_transient_then_succeeds(self):
        route = respx.get(REVERSE).mock(side_effect=[httpx.ConnectError("boom"), _ok()])
        out = fetch.reverse_geocode(39.8, -75.1, base_url=BASE)
        assert out == FC
        assert route.call_count == 2
