"""Fail-open Redis wrapper.

Redis (Upstash in production) is a read-side cache. Any Redis error is logged and
treated as a cache miss, so an outage makes the API slower but never wrong.
"""

import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import TypeVar

import certifi
from redis.asyncio import Redis
from redis.exceptions import RedisError

from notify_queue.config import Settings

log = logging.getLogger(__name__)

T = TypeVar("T")

_CACHE_ERRORS = (RedisError, OSError, asyncio.TimeoutError)


class Cache:
    def __init__(self, redis: Redis | None, prefix: str) -> None:
        self.redis = redis
        self.prefix = prefix

    @classmethod
    def from_settings(cls, settings: Settings) -> "Cache":
        if not settings.cache_enabled:
            return cls(None, settings.redis_key_prefix)
        tls_options: dict[str, str] = {}
        if settings.redis_url.startswith("rediss://"):
            # Use certifi's CA bundle: some Python builds (e.g. Homebrew) ship without
            # system CAs, which makes verification of Upstash's certificate fail.
            tls_options["ssl_ca_certs"] = certifi.where()
        redis = Redis.from_url(
            settings.redis_url,
            decode_responses=True,
            socket_timeout=settings.redis_socket_timeout,
            socket_connect_timeout=settings.redis_connect_timeout,
            **tls_options,
            # Upstash closes idle connections; ping before reusing a stale one.
            health_check_interval=30,
        )
        return cls(redis, settings.redis_key_prefix)

    @property
    def enabled(self) -> bool:
        return self.redis is not None

    def key(self, *parts: object) -> str:
        return self.prefix + ":".join(str(p) for p in parts)

    async def call(self, op: str, fn: Callable[[Redis], Awaitable[T]], default: T) -> T:
        """Run ``fn`` against Redis; on any Redis failure log and return ``default``."""
        if self.redis is None:
            return default
        try:
            return await fn(self.redis)
        except _CACHE_ERRORS as exc:
            log.warning("cache %s failed, falling back to Postgres: %r", op, exc)
            return default

    async def ping(self) -> bool:
        return await self.call("ping", lambda r: r.ping(), False)

    async def close(self) -> None:
        if self.redis is not None:
            await self.redis.aclose()
