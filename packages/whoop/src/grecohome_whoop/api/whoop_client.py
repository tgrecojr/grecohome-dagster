"""Whoop API client for fetching user data.

A client for the Whoop API v2 supporting sleep, workout, recovery, and cycle data
with pagination, rate limiting, and automatic token refresh. Every response's raw
bytes are captured to the bronze layer (capture is always on).
"""

import asyncio
from collections.abc import AsyncIterator
from datetime import datetime
from typing import Any

import httpx
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from grecohome_core.bronze import capture_bronze
from grecohome_core.http.rate_limiter import RateLimiter
from grecohome_core.logging_config import get_logger
from grecohome_whoop import __version__
from grecohome_whoop.auth.token_manager import TokenManager
from grecohome_whoop.config import settings

logger = get_logger(__name__)

# Bronze provenance: identifies this ingest processor in capture sidecars.
_BRONZE_SOURCE = "whoop"
_BRONZE_PROCESSOR = "whoop-ingest"


class WhoopAPIError(Exception):
    """Base exception for Whoop API errors."""

    def __init__(self, message: str, status_code: int | None = None):
        self.status_code = status_code
        super().__init__(message)


class WhoopRetryableError(WhoopAPIError):
    """A transient API error (429 / 5xx) that should be retried."""

    def __init__(
        self,
        message: str,
        status_code: int | None = None,
        retry_after: float | None = None,
    ):
        super().__init__(message, status_code)
        self.retry_after = retry_after


# HTTP statuses worth retrying: rate limiting and transient server errors.
_RETRYABLE_STATUS_CODES = frozenset({429, 500, 502, 503, 504})

# Fallback backoff when the server doesn't tell us how long to wait.
_EXP_WAIT = wait_exponential(multiplier=1, min=2, max=10)

# Hard backstop on cursor pagination. Whoop pages 25 records each; a real per-day
# window is a handful of pages, so 1000 (25k records) is absurdly generous yet
# bounds a misbehaving cursor. Without this, a ``next_token`` the API never clears
# (a known Whoop failure mode) makes ``_paginate`` loop forever — wedging the run
# and, via the ``whoop_api`` pool, every hourly tick behind it.
_MAX_PAGES = 1000


def _parse_retry_after(value: str | None) -> float | None:
    """Parse a Retry-After header (delta-seconds form). HTTP-date form -> None."""
    if not value:
        return None
    try:
        return max(0.0, float(value))
    except (TypeError, ValueError):
        return None


def _is_empty_pagination_terminator(data: Any) -> bool:
    """True if ``data`` is a paginated response carrying zero records.

    Used to skip capturing the "no more results" terminator page (and genuinely
    empty result windows). Single-object endpoints (profile, body measurement)
    have no ``records`` key and never match, so they are always captured.
    """
    return (
        isinstance(data, dict)
        and isinstance(data.get("records"), list)
        and len(data["records"]) == 0
    )


def _whoop_retry_wait(retry_state) -> float:
    """Honor a server-provided Retry-After, else exponential backoff."""
    exc = retry_state.outcome.exception() if retry_state.outcome else None
    if isinstance(exc, WhoopRetryableError) and exc.retry_after is not None:
        return min(exc.retry_after, 60.0)
    return _EXP_WAIT(retry_state)


class WhoopClient:
    """Client for the Whoop API v2.

    Fetches sleep, workout, recovery, and cycle data with automatic pagination,
    rate limiting, and error handling. All responses are captured to bronze.

    Attributes:
        user_id: Logical user id (always 1; single-user system).
        token_manager: Token manager for authentication.
        rate_limiter: In-process rate limiter.
        base_url: Whoop API base URL.
        bronze_dt: Partition date (``YYYY-MM-DD``) to route captures into; when
            None, capture uses the fetch-time UTC date. The Dagster asset sets
            this to its partition key so trailing-day re-captures dedup correctly.
    """

    def __init__(
        self,
        user_id: int = 1,
        token_manager: TokenManager | None = None,
        rate_limiter: RateLimiter | None = None,
        timeout: float = 30.0,
        bronze_dt: str | None = None,
    ) -> None:
        """Initialize the Whoop API client."""
        self.user_id = user_id
        self.token_manager = token_manager or TokenManager()
        self.rate_limiter = rate_limiter or RateLimiter(settings.max_requests_per_minute)
        self.base_url = settings.whoop_api_base_url
        self.timeout = timeout
        self.bronze_dt = bronze_dt
        # Lazily-created, reused across requests for connection pooling/keep-alive.
        self._client: httpx.AsyncClient | None = None

        logger.info("Whoop client initialized", user_id=user_id, base_url=self.base_url)

    def _get_client(self) -> httpx.AsyncClient:
        """Return the shared AsyncClient, creating it on first use."""
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                timeout=self.timeout,
                limits=httpx.Limits(max_connections=10, max_keepalive_connections=5),
            )
        return self._client

    async def aclose(self) -> None:
        """Close the shared HTTP client and release pooled connections."""
        if self._client is not None and not self._client.is_closed:
            await self._client.aclose()
        self._client = None

    async def __aenter__(self) -> WhoopClient:
        return self

    async def __aexit__(self, *exc_info) -> None:
        await self.aclose()

    async def _get_headers(self) -> dict[str, str]:
        """Return HTTP headers with a valid access token.

        Raises:
            WhoopAPIError: If no valid token is available.
        """
        access_token = await self.token_manager.get_valid_token(self.user_id)
        if not access_token:
            raise WhoopAPIError("No valid access token available")
        return {
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json",
        }

    async def _capture_bronze(self, response: httpx.Response, collection: str) -> None:
        """Capture a raw Whoop response to the bronze layer (best-effort).

        Stores the exact response bytes (``response.content`` -- already
        transparently decompressed by httpx, so the stored form is identity) plus
        a provenance sidecar. Runs the blocking file I/O off the event loop and
        never raises.
        """
        try:
            content_type = (response.headers.get("content-type") or "").split(";")[0].strip()
            meta = {
                "request_url": str(response.request.url),
                "request_params": dict(response.request.url.params),
                "http_status": response.status_code,
                "content_type": content_type or None,
                "charset": response.charset_encoding,
                "content_encoding": response.headers.get("content-encoding", "identity"),
                "stored_encoding": "identity",
                "processor": _BRONZE_PROCESSOR,
                "processor_version": __version__,
            }
            await asyncio.to_thread(
                capture_bronze,
                _BRONZE_SOURCE,
                collection,
                response.content,
                meta,
                bronze_root=settings.bronze_root,
                dt=self.bronze_dt,
            )
        except Exception as e:  # noqa: BLE001 - capture must never break processing
            logger.warning("bronze dispatch failed", collection=collection, error=str(e))

    @retry(
        stop=stop_after_attempt(3),
        wait=_whoop_retry_wait,
        retry=retry_if_exception_type(
            (httpx.TimeoutException, httpx.NetworkError, WhoopRetryableError)
        ),
        reraise=True,
    )
    async def _make_request(
        self,
        endpoint: str,
        params: dict[str, Any] | None = None,
        collection: str | None = None,
    ) -> dict[str, Any]:
        """Make an authenticated request to the Whoop API, with retries.

        When ``collection`` is set, the raw bytes of a **successful** response are
        captured to bronze (excluding empty pagination terminators). Error
        responses are never persisted -- only their status is logged -- so an auth
        blip (e.g. a 401 ``Authorization was not valid`` body) can't land in bronze
        and trip the profile content/integrity check.

        Raises:
            WhoopAPIError: If the request fails after retries.
        """
        await self.rate_limiter.acquire()

        url = f"{self.base_url}{endpoint}"
        headers = await self._get_headers()

        logger.debug("Making API request", endpoint=endpoint, params=params)

        try:
            client = self._get_client()
            response = await client.get(url, headers=headers, params=params)

            response.raise_for_status()
            data = response.json()

            # Capture successful payloads only, skipping empty pagination terminators.
            # Error responses are handled below (status logged, body never stored).
            capture = bool(collection) and bool(response.content)
            if capture and not _is_empty_pagination_terminator(data):
                await self._capture_bronze(response, collection)

            logger.debug(
                "API request successful",
                endpoint=endpoint,
                status_code=response.status_code,
            )
            return data

        except httpx.HTTPStatusError as e:
            status_code = e.response.status_code
            # Log status only -- never the response body.
            if status_code in _RETRYABLE_STATUS_CODES:
                retry_after = _parse_retry_after(e.response.headers.get("Retry-After"))
                logger.warning(
                    "Retryable API error, will retry",
                    endpoint=endpoint,
                    status_code=status_code,
                    retry_after=retry_after,
                )
                raise WhoopRetryableError(
                    f"Retryable API error (status {status_code})",
                    status_code=status_code,
                    retry_after=retry_after,
                ) from e
            logger.error("API request failed", endpoint=endpoint, status_code=status_code)
            raise WhoopAPIError(
                f"API request failed with status {status_code}",
                status_code=status_code,
            ) from e
        except (httpx.TimeoutException, httpx.NetworkError) as e:
            logger.warning("Network error, will retry", endpoint=endpoint, error=str(e))
            raise
        except WhoopAPIError:
            raise
        except Exception as e:
            logger.error(
                "Unexpected error during API request",
                endpoint=endpoint,
                error=str(e),
                exc_info=True,
            )
            raise WhoopAPIError(f"Unexpected error: {str(e)}") from e

    async def _paginate(
        self,
        endpoint: str,
        params: dict[str, Any] | None = None,
        limit: int = 25,
        collection: str | None = None,
        max_pages: int = _MAX_PAGES,
    ) -> AsyncIterator[dict[str, Any]]:
        """Paginate through API results (cursor-based via ``next_token``).

        Guarded against a runaway cursor: stops if ``next_token`` fails to advance
        (repeats the previous token or one already seen) or if ``max_pages`` is hit.
        Either is a Whoop-side failure (a cursor that never clears), not normal
        end-of-results, so it's logged at WARNING — but it still terminates the loop
        so the run finishes and releases the ``whoop_api`` pool instead of wedging.
        """
        params = params or {}
        params["limit"] = min(limit, 25)  # Whoop max is 25
        next_token = None
        page_count = 0
        seen_tokens: set[str] = set()

        while True:
            page_count += 1
            if next_token:
                params["nextToken"] = next_token

            response = await self._make_request(endpoint, params, collection=collection)

            records = response.get("records", [])
            next_token = response.get("next_token")

            logger.info(
                "Fetched page",
                endpoint=endpoint,
                page=page_count,
                records_count=len(records),
                has_next=bool(next_token),
            )

            for record in records:
                yield record

            # Normal termination: no further cursor, or an empty page.
            if not next_token or not records:
                logger.info("Pagination complete", endpoint=endpoint, total_pages=page_count)
                break

            # Runaway-cursor guards (Whoop sometimes returns a non-clearing token).
            if next_token in seen_tokens:
                logger.warning(
                    "Pagination cursor did not advance; stopping",
                    endpoint=endpoint,
                    page=page_count,
                )
                break
            seen_tokens.add(next_token)

            if page_count >= max_pages:
                logger.warning(
                    "Pagination hit max_pages cap; stopping (possible runaway cursor)",
                    endpoint=endpoint,
                    max_pages=max_pages,
                )
                break

    async def _fetch_range(
        self,
        endpoint: str,
        collection: str,
        start: datetime | None,
        end: datetime | None,
        limit: int,
    ) -> list[dict[str, Any]]:
        """Fetch a paginated, time-windowed collection into a list."""
        params: dict[str, Any] = {}
        if start:
            params["start"] = start.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        if end:
            params["end"] = end.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

        logger.info(
            f"Fetching {collection} records",
            user_id=self.user_id,
            start=start.isoformat() if start else None,
            end=end.isoformat() if end else None,
        )

        records = [
            record
            async for record in self._paginate(endpoint, params, limit, collection=collection)
        ]
        logger.info(f"Fetched {collection} records", user_id=self.user_id, count=len(records))
        return records

    async def get_sleep_records(
        self,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int = 25,
    ) -> list[dict[str, Any]]:
        """Fetch sleep records for the time window."""
        return await self._fetch_range("/developer/v2/activity/sleep", "sleep", start, end, limit)

    async def get_workout_records(
        self,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int = 25,
    ) -> list[dict[str, Any]]:
        """Fetch workout records for the time window."""
        return await self._fetch_range(
            "/developer/v2/activity/workout", "workout", start, end, limit
        )

    async def get_recovery_records(
        self,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int = 25,
    ) -> list[dict[str, Any]]:
        """Fetch recovery records for the time window."""
        return await self._fetch_range("/developer/v2/recovery", "recovery", start, end, limit)

    async def get_cycle_records(
        self,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int = 25,
    ) -> list[dict[str, Any]]:
        """Fetch cycle records for the time window."""
        return await self._fetch_range("/developer/v2/cycle", "cycle", start, end, limit)

    async def get_user_profile(self) -> dict[str, Any]:
        """Fetch user profile information (single-object endpoint)."""
        logger.info("Fetching user profile", user_id=self.user_id)
        return await self._make_request("/developer/v2/user/profile/basic", collection="profile")

    async def get_body_measurement(self) -> dict[str, Any]:
        """Fetch user body measurements (single-object; needs read:body_measurement)."""
        logger.info("Fetching body measurement", user_id=self.user_id)
        return await self._make_request(
            "/developer/v2/user/measurement/body", collection="body_measurement"
        )
