"""Per-recipient rate limiting: a Redis sliding window, with Postgres as fallback.

Redis (primary): one sorted set per recipient, one member per job that holds a
slot, scored by when it took the slot. A Lua script trims expired members, then
admits the job only if fewer than ``limit`` remain. It runs atomically inside
Redis and reads Redis's own clock, so every worker sees the same window. Because
the member is the job id:

  * a job that is reclaimed after a worker crash reuses its own slot instead of
    taking a second one;
  * a slot can be handed back when a send definitely failed.

Postgres (fallback): the fixed-window counter in ``rate_limit_buckets``, used when
Redis is disabled or unreachable. During a Redis outage the two limiters don't
share counts, so a recipient can get up to 2x the limit within one window. That
is the price of staying available. Duplicate sends are never affected.
"""

import logging
from typing import Protocol

from sqlalchemy.ext.asyncio import AsyncEngine

from notify_queue.cache.client import Cache
from notify_queue.config import Settings
from notify_queue.domain.models import Job
from notify_queue.repositories import jobs as jobs_repo

log = logging.getLogger(__name__)

# KEYS[1] recipient key; ARGV[1] window ms; ARGV[2] limit; ARGV[3] job id.
# Returns {1, 0} when admitted, or {0, ms until the oldest slot frees up}.
_ACQUIRE = """
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local window = tonumber(ARGV[1])
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now - window)
if redis.call('ZSCORE', KEYS[1], ARGV[3]) then
  return {1, 0}
end
if redis.call('ZCARD', KEYS[1]) < tonumber(ARGV[2]) then
  redis.call('ZADD', KEYS[1], now, ARGV[3])
  redis.call('PEXPIRE', KEYS[1], window)
  return {1, 0}
end
local oldest = redis.call('ZRANGE', KEYS[1], 0, 0, 'WITHSCORES')
return {0, tonumber(oldest[2]) + window - now}
"""


class RateLimiter(Protocol):
    async def acquire(self, job: Job) -> float | None:
        """Take a slot for ``job``. Returns ``None`` if it may send now, otherwise
        how many seconds to wait before trying again."""
        ...

    async def release(self, job: Job) -> None:
        """Give back ``job``'s slot. Only call this when the send definitely did not
        deliver, otherwise the recipient could receive more than the limit."""
        ...


class PostgresRateLimiter:
    def __init__(self, engine: AsyncEngine, settings: Settings) -> None:
        self._engine = engine
        self._settings = settings

    async def acquire(self, job: Job) -> float | None:
        async with self._engine.begin() as conn:
            return await jobs_repo.try_acquire_rate_limit(
                conn,
                recipient=job.recipient,
                limit=self._settings.rate_limit_per_hour,
                window_seconds=self._settings.rate_limit_window_seconds,
            )

    async def release(self, job: Job) -> None:
        # Fixed-window counters are not refunded: by the time a send fails the
        # window may have rolled over, and decrementing the wrong one would allow
        # an over-send.
        return None


class RedisRateLimiter:
    def __init__(self, cache: Cache, settings: Settings, fallback: PostgresRateLimiter) -> None:
        assert cache.redis is not None
        self._cache = cache
        self._settings = settings
        self._fallback = fallback
        self._acquire = cache.redis.register_script(_ACQUIRE)
        self._window_ms = int(settings.rate_limit_window_seconds * 1000)

    def _key(self, recipient: str) -> str:
        return self._cache.key("rl", recipient)

    async def acquire(self, job: Job) -> float | None:
        script = self._acquire
        # The script always returns a list, so None can only mean Redis was unreachable.
        result: list[int] | None = await self._cache.call(
            "rate limit acquire",
            lambda r: script(
                keys=[self._key(job.recipient)],
                args=[self._window_ms, self._settings.rate_limit_per_hour, str(job.id)],
                client=r,
            ),
            None,
        )
        if result is None:
            log.warning("Redis unavailable; rate limiting %s with Postgres", job.recipient)
            return await self._fallback.acquire(job)
        admitted, wait_ms = result
        return None if admitted else max(wait_ms, 1) / 1000

    async def release(self, job: Job) -> None:
        await self._cache.call(
            "rate limit release",
            lambda r: r.zrem(self._key(job.recipient), str(job.id)),
            None,
        )


def build_rate_limiter(engine: AsyncEngine, cache: Cache, settings: Settings) -> RateLimiter:
    postgres = PostgresRateLimiter(engine, settings)
    if settings.rate_limit_backend == "redis" and cache.enabled:
        return RedisRateLimiter(cache, settings, fallback=postgres)
    return postgres
