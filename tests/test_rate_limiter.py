import asyncio
from collections.abc import AsyncIterator

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine

from notify_queue.cache.client import Cache
from notify_queue.config import Settings
from notify_queue.services.rate_limiter import (
    PostgresRateLimiter,
    RedisRateLimiter,
    build_rate_limiter,
)
from tests.conftest import make_settings
from tests.factories import insert_jobs, scalar


@pytest.fixture
def limiter_settings() -> Settings:
    return make_settings(rate_limit_per_hour=3, rate_limit_window_seconds=1)


@pytest.fixture
async def redis_limiter(
    engine: AsyncEngine, limiter_settings: Settings
) -> AsyncIterator[RedisRateLimiter]:
    cache = Cache.from_settings(limiter_settings)
    limiter = build_rate_limiter(engine, cache, limiter_settings)
    assert isinstance(limiter, RedisRateLimiter)
    yield limiter
    await cache.close()


async def test_admits_up_to_the_limit_then_reports_wait(
    engine: AsyncEngine, redis_limiter: RedisRateLimiter
) -> None:
    jobs = await insert_jobs(engine, 4, recipient="r@example.com")

    results = [await redis_limiter.acquire(job) for job in jobs]

    assert results[:3] == [None, None, None]
    assert results[3] is not None and 0 < results[3] <= 1.0


async def test_window_slides(engine: AsyncEngine, redis_limiter: RedisRateLimiter) -> None:
    jobs = await insert_jobs(engine, 4, recipient="r@example.com")
    for job in jobs[:3]:
        assert await redis_limiter.acquire(job) is None
    wait = await redis_limiter.acquire(jobs[3])
    assert wait is not None

    await asyncio.sleep(wait + 0.05)

    assert await redis_limiter.acquire(jobs[3]) is None


async def test_same_job_reuses_its_slot(
    engine: AsyncEngine, redis_limiter: RedisRateLimiter
) -> None:
    """A job reclaimed after a worker crash must not take a second slot."""
    jobs = await insert_jobs(engine, 3, recipient="r@example.com")

    for _ in range(5):
        assert await redis_limiter.acquire(jobs[0]) is None

    assert await redis_limiter.acquire(jobs[1]) is None
    assert await redis_limiter.acquire(jobs[2]) is None


async def test_release_frees_the_slot(engine: AsyncEngine, redis_limiter: RedisRateLimiter) -> None:
    jobs = await insert_jobs(engine, 4, recipient="r@example.com")
    for job in jobs[:3]:
        await redis_limiter.acquire(job)

    await redis_limiter.release(jobs[0])

    assert await redis_limiter.acquire(jobs[3]) is None


async def test_recipients_are_limited_independently(
    engine: AsyncEngine, redis_limiter: RedisRateLimiter
) -> None:
    busy = await insert_jobs(engine, 3, recipient="busy@example.com")
    [other] = await insert_jobs(engine, 1, recipient="other@example.com")
    for job in busy:
        await redis_limiter.acquire(job)

    assert await redis_limiter.acquire(other) is None


async def test_concurrent_acquires_never_exceed_the_limit(engine: AsyncEngine) -> None:
    settings = make_settings(rate_limit_per_hour=5)
    cache = Cache.from_settings(settings)
    limiter = build_rate_limiter(engine, cache, settings)
    jobs = await insert_jobs(engine, 40, recipient="r@example.com")

    results = await asyncio.gather(*(limiter.acquire(job) for job in jobs))
    await cache.close()

    assert sum(result is None for result in results) == 5


async def test_falls_back_to_postgres_when_redis_is_down(engine: AsyncEngine) -> None:
    settings = make_settings(
        redis_url="redis://localhost:1/0",
        redis_socket_timeout=0.2,
        redis_connect_timeout=0.2,
        rate_limit_per_hour=2,
    )
    cache = Cache.from_settings(settings)
    limiter = build_rate_limiter(engine, cache, settings)
    jobs = await insert_jobs(engine, 3, recipient="r@example.com")

    results = [await limiter.acquire(job) for job in jobs]
    await cache.close()

    assert results[:2] == [None, None] and results[2] is not None
    assert await scalar(engine, "SELECT count FROM rate_limit_buckets") == 2


async def test_postgres_backend_when_configured_or_cache_disabled(engine: AsyncEngine) -> None:
    for settings in (
        make_settings(rate_limit_backend="postgres"),
        make_settings(cache_enabled=False),
    ):
        cache = Cache.from_settings(settings)
        assert isinstance(build_rate_limiter(engine, cache, settings), PostgresRateLimiter)
        await cache.close()
