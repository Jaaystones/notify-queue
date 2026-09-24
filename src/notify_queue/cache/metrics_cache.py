"""Short-TTL cache for the metrics endpoint.

Metrics are a full scan of ``jobs``, so a dashboard polled by many clients would
otherwise run one scan per request. With a 2s TTL at most one scan runs per TTL,
and a ``SET NX`` lock stops a burst of requests arriving just after expiry from all
rebuilding at once.
"""

import asyncio
import json
from collections.abc import Awaitable, Callable
from typing import Any

from notify_queue.cache.client import Cache

LOCK_TTL_SECONDS = 5
LOCK_WAIT_SECONDS = 0.1


class MetricsCache:
    def __init__(self, cache: Cache, ttl: int) -> None:
        self._cache = cache
        self._ttl = ttl
        self._key = cache.key("metrics", "v1")
        self._lock_key = cache.key("metrics", "v1", "lock")

    async def get_or_compute(
        self, compute: Callable[[], Awaitable[dict[str, Any]]]
    ) -> tuple[dict[str, Any], bool]:
        """Returns ``(metrics, from_cache)``."""
        cached = await self._get()
        if cached is not None:
            return cached, True

        got_lock = await self._cache.call(
            "metrics lock", lambda r: r.set(self._lock_key, "1", nx=True, ex=LOCK_TTL_SECONDS), None
        )
        if not got_lock and self._cache.enabled:
            # Someone else is rebuilding; give them a moment, then compute anyway
            # rather than make the caller wait on a lock holder that may have died.
            await asyncio.sleep(LOCK_WAIT_SECONDS)
            cached = await self._get()
            if cached is not None:
                return cached, True

        metrics = await compute()
        await self._cache.call(
            "set metrics",
            lambda r: r.set(self._key, json.dumps(metrics, default=str), ex=self._ttl),
            None,
        )
        if got_lock:
            await self._cache.call("metrics unlock", lambda r: r.delete(self._lock_key), None)
        return metrics, False

    async def _get(self) -> dict[str, Any] | None:
        raw = await self._cache.call("get metrics", lambda r: r.get(self._key), None)
        return json.loads(raw) if raw else None
