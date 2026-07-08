"""Location-specific silver column mapping — points enriched with reverse-geocoded place.

Two bronze point streams normalized into one typed table, then LEFT JOINed to the
``geocode`` bronze cache to attach place context. A pure DuckDB projection over the
filesystem — **no network at transform time** (the Photon calls already happened in the
geocode bronze cache), so a rebuild is cheap and offline.

Source shapes (profiled against live bronze):

* **Overland** — one object per file: ``{"locations": [Feature, ...]}``. Each Feature is
  GeoJSON: ``geometry.coordinates = [lon, lat]`` (longitude first) and
  ``properties.timestamp`` (ISO-8601, UTC). ``properties.horizontal_accuracy`` is metres.
* **OwnTracks** — one message per file. Location messages carry flat ``lat``/``lon`` and
  ``tst`` (epoch **seconds**); ``acc`` is metres. Non-location messages (``lwt`` /
  ``transition`` without coordinates) are dropped.

Cell key: each point is snapped to the same integer 1e-4-degree cell as the geocode
cache — ``CAST(round(coord * 10000) AS BIGINT)`` — and the cache is joined on
``(lat_e4, lon_e4)``. This mirrors :func:`grecohome_geocode.cells.snap_e4` exactly
(DuckDB ``round()`` rounds half away from zero, as does that helper).

The geocode cache's cell key lives in the sidecar (the raw Photon body has no notion of
our grid), so the cache relation reads the ``.meta.json`` sidecars for ``lat_e4``/
``lon_e4`` and joins them to their payloads (``features[0]`` = nearest match) by filename.
"""

from __future__ import annotations

from grecohome_core.silver import dedup_latest_sql, json_num, json_str, payloads_relation_sql

# Bronze filename carries the 13-digit fetch-millis; latest capture wins a tie for a
# point/cell (NULLs sort last in dedup_latest_sql).
_FETCHED_MS = r"TRY_CAST(regexp_extract(filename, '_([0-9]{13})_', 1) AS BIGINT)"


def _e4(expr: str) -> str:
    """Integer 1e-4-degree cell index for a coordinate expression (matches snap_e4)."""
    return f"CAST(round({expr} * 10000) AS BIGINT)"


# ---------------------------------------------------------------------------
# Point normalization (per stream)
# ---------------------------------------------------------------------------
def _overland_points_sql(payloads_sql: str) -> str:
    """One row per Overland fix: unnest ``$.locations``, read [lon, lat] + timestamp."""
    records = (
        "SELECT p.filename AS filename, loc AS r "
        f"FROM ({payloads_sql}) AS p, "
        "UNNEST(CAST(p.j -> '$.locations' AS JSON[])) AS t(loc)"
    )
    return f"""
        SELECT
            'overland'                                              AS source_stream,
            TRY_CAST({json_str('r', 'properties.timestamp')} AS TIMESTAMP) AS event_ts_utc,
            {json_num('r', 'geometry.coordinates[1]')}             AS lat,
            {json_num('r', 'geometry.coordinates[0]')}             AS lon,
            {json_num('r', 'properties.horizontal_accuracy')}      AS accuracy_m,
            {_FETCHED_MS}                                          AS _fetched_ms
        FROM ({records})
        WHERE {json_num('r', 'geometry.coordinates[1]')} IS NOT NULL
          AND {json_num('r', 'geometry.coordinates[0]')} IS NOT NULL
          AND TRY_CAST({json_str('r', 'properties.timestamp')} AS TIMESTAMP) IS NOT NULL
    """


def _owntracks_points_sql(payloads_sql: str) -> str:
    """One row per OwnTracks location message: flat lat/lon + ``tst`` (epoch seconds)."""
    return f"""
        SELECT
            'owntracks'                                            AS source_stream,
            make_timestamp(TRY_CAST({json_str('j', 'tst')} AS BIGINT) * 1000000)
                                                                   AS event_ts_utc,
            {json_num('j', 'lat')}                                 AS lat,
            {json_num('j', 'lon')}                                 AS lon,
            {json_num('j', 'acc')}                                 AS accuracy_m,
            {_FETCHED_MS}                                          AS _fetched_ms
        FROM ({payloads_sql})
        WHERE {json_num('j', 'lat')} IS NOT NULL
          AND {json_num('j', 'lon')} IS NOT NULL
          AND TRY_CAST({json_str('j', 'tst')} AS BIGINT) IS NOT NULL
    """


def points_sql(overland_files: list[str], owntracks_files: list[str]) -> str:
    """Unified, typed, deduped points from both streams (one row per fix)."""
    ov = _overland_points_sql(payloads_relation_sql(overland_files))
    ot = _owntracks_points_sql(payloads_relation_sql(owntracks_files))
    typed = f"""
        SELECT
            source_stream, event_ts_utc, lat, lon, accuracy_m,
            {_e4('lat')}                  AS lat_e4,
            {_e4('lon')}                  AS lon_e4,
            CAST(event_ts_utc AS DATE)    AS event_date_utc,
            _fetched_ms
        FROM (({ov}) UNION ALL ({ot}))
    """
    # A fix's identity is (stream, instant, position): a re-promoted byte-identical POST
    # collapses to one row (latest capture wins), while two genuinely distinct fixes that
    # happen to share a second stay separate.
    return dedup_latest_sql(
        typed, partition_key="source_stream, event_ts_utc, lat, lon", order_by="_fetched_ms"
    )


# ---------------------------------------------------------------------------
# Geocode cache relation (one row per cell — the nearest Photon match)
# ---------------------------------------------------------------------------
#: Photon feature ``properties`` fields carried into silver (all optional per location).
_GEO_FIELDS = {
    "geo_name": "name",
    "geo_house_number": "housenumber",
    "geo_street": "street",
    "geo_city": "city",
    "geo_district": "district",
    "geo_county": "county",
    "geo_state": "state",
    "geo_postcode": "postcode",
    "geo_country": "country",
    "geo_country_code": "countrycode",
    "geo_osm_key": "osm_key",
    "geo_osm_value": "osm_value",
    "geo_osm_type": "osm_type",
}


def geocode_cache_sql(payload_files: list[str], sidecar_files: list[str]) -> str:
    """One row per cell: ``(lat_e4, lon_e4)`` + the nearest Photon match's place fields.

    The cell key lives in the sidecar; place fields live in the payload's ``features[0]``.
    We join sidecar→payload by filename (``…​.meta.json`` → ``….json``) and dedup to the
    latest capture per cell.
    """
    payloads = payloads_relation_sql(payload_files)
    sidecars = payloads_relation_sql(sidecar_files)
    props = ",\n            ".join(
        f"(fr.f0 ->> '$.properties.{src}') AS {alias}"
        for alias, src in _GEO_FIELDS.items()
    )
    inner = f"""
        SELECT
            s.lat_e4                                          AS lat_e4,
            s.lon_e4                                          AS lon_e4,
            {props},
            TRY_CAST(fr.f0 ->> '$.properties.osm_id' AS BIGINT) AS geo_osm_id,
            s._fetched_ms                                     AS _fetched_ms
        FROM (
            SELECT
                regexp_replace(filename, '\\.meta\\.json$', '.json') AS payload_file,
                TRY_CAST(j ->> '$.lat_e4' AS BIGINT)  AS lat_e4,
                TRY_CAST(j ->> '$.lon_e4' AS BIGINT)  AS lon_e4,
                TRY_CAST(j ->> '$.fetched_at_unix_ms' AS BIGINT) AS _fetched_ms
            FROM ({sidecars})
        ) AS s
        JOIN (
            SELECT filename AS payload_file, (j -> '$.features[0]') AS f0
            FROM ({payloads})
        ) AS fr ON s.payload_file = fr.payload_file
        WHERE s.lat_e4 IS NOT NULL AND s.lon_e4 IS NOT NULL
    """
    deduped = dedup_latest_sql(inner, partition_key="lat_e4, lon_e4", order_by="_fetched_ms")
    return f"SELECT * EXCLUDE (_fetched_ms) FROM ({deduped})"


# ---------------------------------------------------------------------------
# Unified: points LEFT JOIN cache on the cell
# ---------------------------------------------------------------------------
def location_sql(
    overland_files: list[str],
    owntracks_files: list[str],
    geocode_payload_files: list[str],
    geocode_sidecar_files: list[str],
) -> str:
    """Enriched location points — one row per fix, place fields LEFT JOINed by cell."""
    pts = points_sql(overland_files, owntracks_files)
    cache = geocode_cache_sql(geocode_payload_files, geocode_sidecar_files)
    geo_cols = ",\n            ".join(f"c.{alias}" for alias in _GEO_FIELDS)
    return f"""
        SELECT
            p.event_ts_utc,
            p.source_stream,
            p.lat,
            p.lon,
            p.lat_e4,
            p.lon_e4,
            p.accuracy_m,
            p.event_date_utc,
            (c.lat_e4 IS NOT NULL) AS geocoded,
            {geo_cols},
            c.geo_osm_id
        FROM ({pts}) AS p
        LEFT JOIN ({cache}) AS c
            ON p.lat_e4 = c.lat_e4 AND p.lon_e4 = c.lon_e4
        ORDER BY p.event_ts_utc, p.source_stream, p.lat_e4, p.lon_e4
    """


def bronze_point_count_sql(overland_files: list[str], owntracks_files: list[str]) -> str:
    """Count of distinct bronze fixes (stream, instant) — for the coverage check."""
    pts = points_sql(overland_files, owntracks_files)
    return f"SELECT count(*) AS n FROM ({pts})"
