"""Sliding-window rate limiter for source-API requests.

In-process, async-safe limiter that enforces a per-minute request cap. It is the
within-run guard against bursting past a source's API limit; cross-run
serialization (e.g. across separate Dagster run processes) is handled by a
Dagster concurrency pool, not here.
"""

import asyncio
from collections import deque
from datetime import UTC, datetime, timedelta

from grecohome_core.logging_config import get_logger

logger = get_logger(__name__)


class RateLimiter:
    """Sliding-window rate limiter for API requests.

    Tracks request timestamps and delays new requests when the per-minute cap
    would be exceeded. Async-safe via an internal lock.

    Attributes:
        max_requests: Maximum requests allowed per window (after the safety margin).
        window_seconds: Length of the sliding window in seconds.
        requests: Deque of recent request timestamps.
        lock: Asyncio lock guarding the deque.
    """

    def __init__(
        self,
        max_requests_per_minute: int = 60,
        safety_margin: float = 0.9,
    ) -> None:
        """Initialize the limiter.

        Args:
            max_requests_per_minute: Source API per-minute cap.
            safety_margin: Fraction of the cap to actually use (0.9 = 90%) for headroom.
        """
        # Guard against a config that floors to 0, which would make acquire()
        # loop forever and get_stats() divide by zero.
        self.max_requests = max(1, int(max_requests_per_minute * safety_margin))
        self.window_seconds = 60
        self.requests: deque[datetime] = deque()
        self.lock = asyncio.Lock()

        logger.info(
            "Rate limiter initialized",
            max_requests=self.max_requests,
            window_seconds=self.window_seconds,
            safety_margin=safety_margin,
        )

    async def acquire(self) -> None:
        """Acquire permission to make a request, blocking until a slot is free.

        Call this before every API request.
        """
        # Loop so that after waiting we re-check capacity under the lock before
        # recording. A single `if` would let several woken coroutines all append
        # at once and overshoot max_requests.
        while True:
            async with self.lock:
                now = datetime.now(UTC)
                self._cleanup_old_requests(now)

                if len(self.requests) < self.max_requests:
                    # A slot is free: record it while still holding the lock so
                    # no other coroutine can claim the same slot.
                    self.requests.append(now)
                    logger.debug(
                        "Rate limit acquired",
                        requests_in_window=len(self.requests),
                        max_requests=self.max_requests,
                        utilization_percent=(len(self.requests) / self.max_requests) * 100,
                    )
                    return

                # At the limit: compute how long until the oldest request leaves
                # the window, then release the lock and wait before retrying.
                oldest_request = self.requests[0]
                window_start = now - timedelta(seconds=self.window_seconds)
                wait_time = (oldest_request - window_start).total_seconds() + 0.1

                logger.warning(
                    "Rate limit reached, waiting",
                    wait_seconds=wait_time,
                    requests_in_window=len(self.requests),
                    max_requests=self.max_requests,
                )

            # Lock released here. Sleep outside the lock so other coroutines can
            # make progress, then loop to re-acquire and re-check.
            await asyncio.sleep(max(wait_time, 0))

    def _cleanup_old_requests(self, now: datetime) -> None:
        """Drop request timestamps that have left the current window."""
        window_start = now - timedelta(seconds=self.window_seconds)
        while self.requests and self.requests[0] < window_start:
            self.requests.popleft()

    async def get_stats(self) -> dict:
        """Return current limiter statistics."""
        async with self.lock:
            now = datetime.now(UTC)
            self._cleanup_old_requests(now)
            return {
                "requests_in_window": len(self.requests),
                "max_requests": self.max_requests,
                "window_seconds": self.window_seconds,
                "utilization_percent": (len(self.requests) / self.max_requests) * 100,
                "available_slots": self.max_requests - len(self.requests),
            }

    async def reset(self) -> None:
        """Clear all tracked requests."""
        async with self.lock:
            self.requests.clear()
            logger.info("Rate limiter reset")

    def __repr__(self) -> str:
        return (
            f"RateLimiter(max_requests={self.max_requests}, "
            f"window_seconds={self.window_seconds}, "
            f"current_requests={len(self.requests)})"
        )


class RateLimitExceeded(Exception):
    """Raised when a rate limit is exceeded.

    Not raised in normal operation (the limiter waits instead); provided for
    testing and edge cases.
    """

    def __init__(self, message: str = "Rate limit exceeded", retry_after: float = 60.0):
        self.retry_after = retry_after
        super().__init__(f"{message} (retry after {retry_after}s)")
