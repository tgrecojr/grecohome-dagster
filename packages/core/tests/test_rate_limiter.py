"""Tests for the rate limiter (grecohome_core.http.rate_limiter)."""

import asyncio
from collections import deque
from datetime import UTC, datetime, timedelta

import pytest

from grecohome_core.http.rate_limiter import RateLimiter


class _PeakDeque(deque):
    """A deque that records the maximum length ever reached."""

    def __init__(self, *args):
        super().__init__(*args)
        self.peak = 0

    def append(self, item):
        super().append(item)
        if len(self) > self.peak:
            self.peak = len(self)


@pytest.mark.unit
@pytest.mark.asyncio
class TestRateLimiter:
    async def test_initialization(self):
        limiter = RateLimiter(max_requests_per_minute=60, safety_margin=0.9)
        assert limiter.max_requests == 54
        assert limiter.window_seconds == 60
        assert len(limiter.requests) == 0

    async def test_acquire_single_request(self):
        limiter = RateLimiter(max_requests_per_minute=60)
        await limiter.acquire()
        assert len(limiter.requests) == 1

    async def test_acquire_multiple_requests(self):
        limiter = RateLimiter(max_requests_per_minute=60)
        for _ in range(10):
            await limiter.acquire()
        assert len(limiter.requests) == 10

    async def test_rate_limit_not_exceeded_within_limit(self):
        limiter = RateLimiter(max_requests_per_minute=10)
        start = datetime.now()
        for _ in range(5):
            await limiter.acquire()
        assert (datetime.now() - start).total_seconds() < 1.0
        assert len(limiter.requests) == 5

    @pytest.mark.slow
    async def test_rate_limit_enforced(self):
        limiter = RateLimiter(max_requests_per_minute=5, safety_margin=1.0)
        start = datetime.now()
        for _ in range(7):
            await limiter.acquire()
        assert (datetime.now() - start).total_seconds() > 30
        assert len(limiter.requests) <= limiter.max_requests

    async def test_get_stats(self):
        limiter = RateLimiter(max_requests_per_minute=60)
        for _ in range(10):
            await limiter.acquire()
        stats = await limiter.get_stats()
        assert stats["requests_in_window"] == 10
        assert stats["max_requests"] == 54
        assert stats["window_seconds"] == 60
        assert stats["available_slots"] == 44
        assert stats["utilization_percent"] == pytest.approx(18.52, abs=0.1)

    async def test_reset(self):
        limiter = RateLimiter(max_requests_per_minute=60)
        for _ in range(10):
            await limiter.acquire()
        assert len(limiter.requests) == 10
        await limiter.reset()
        assert len(limiter.requests) == 0

    async def test_cleanup_old_requests(self):
        limiter = RateLimiter(max_requests_per_minute=60)
        now = datetime.now(UTC)
        old = now - timedelta(seconds=70)
        limiter.requests.extend([old, old, now])
        await limiter.acquire()
        assert len(limiter.requests) == 2

    async def test_concurrent_requests(self):
        limiter = RateLimiter(max_requests_per_minute=20)
        await asyncio.gather(*[limiter.acquire() for _ in range(15)])
        stats = await limiter.get_stats()
        assert stats["requests_in_window"] == 15

    async def test_never_overshoots_max_under_concurrency(self):
        limiter = RateLimiter(max_requests_per_minute=2, safety_margin=1.0)
        assert limiter.max_requests == 2
        limiter.window_seconds = 1
        limiter.requests = _PeakDeque()
        now = datetime.now(UTC)
        limiter.requests.append(now - timedelta(seconds=0.9))
        limiter.requests.append(now - timedelta(seconds=0.1))
        await asyncio.gather(*[limiter.acquire() for _ in range(3)])
        assert limiter.requests.peak <= limiter.max_requests
        assert len(limiter.requests) <= limiter.max_requests

    async def test_max_requests_floors_to_at_least_one(self):
        limiter = RateLimiter(max_requests_per_minute=1, safety_margin=0.1)
        assert limiter.max_requests == 1

    async def test_custom_safety_margin(self):
        limiter = RateLimiter(max_requests_per_minute=100, safety_margin=0.5)
        assert limiter.max_requests == 50

    async def test_repr(self):
        limiter = RateLimiter(max_requests_per_minute=60)
        r = repr(limiter)
        assert "RateLimiter" in r
        assert "max_requests=54" in r
        assert "window_seconds=60" in r
