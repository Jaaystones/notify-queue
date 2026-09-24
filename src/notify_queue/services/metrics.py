from datetime import UTC, datetime
from typing import Any

from sqlalchemy.ext.asyncio import AsyncEngine

from notify_queue.cache.metrics_cache import MetricsCache
from notify_queue.repositories import jobs as jobs_repo


class MetricsService:
    def __init__(self, engine: AsyncEngine, metrics_cache: MetricsCache) -> None:
        self._engine = engine
        self._cache = metrics_cache

    async def snapshot(self) -> dict[str, Any]:
        metrics, from_cache = await self._cache.get_or_compute(self._compute)
        return {**metrics, "cached": from_cache}

    async def _compute(self) -> dict[str, Any]:
        async with self._engine.connect() as conn:
            counts = await jobs_repo.job_counts(conn)
        return {
            # Every job still waiting in the queue. Breakdown (overlapping):
            # pending_due - claimable now; scheduled - future, not failed yet;
            # failed - last attempt failed, waiting to retry.
            "pending": counts["pending"],
            "pending_due": counts["pending_due"],
            "scheduled": counts["scheduled"],
            "failed": counts["retrying"],
            "processing": counts["processing"],
            "sent": counts["sent"],
            "failed_attempts_total": counts["failed_attempts"],
            "dead_lettered": counts["dead_lettered"],
            "webhooks_pending": counts["webhooks_pending"],
            "generated_at": datetime.now(UTC).isoformat(),
        }
