"""Returns jobs held by dead workers to the queue.

Runs inside every worker process. SKIP LOCKED makes concurrent reapers safe, so
there is no single reaper to fail.
"""

import asyncio
import logging

from sqlalchemy.ext.asyncio import AsyncEngine

from notify_queue.config import Settings
from notify_queue.repositories import jobs as jobs_repo
from notify_queue.repositories.jobs import Transition
from notify_queue.worker.common import CacheInvalidator, sleep_or_stop

log = logging.getLogger(__name__)


class Reaper:
    def __init__(
        self, engine: AsyncEngine, settings: Settings, invalidator: CacheInvalidator
    ) -> None:
        self._engine = engine
        self._settings = settings
        self._invalidator = invalidator

    async def run(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            try:
                await self.run_once()
            except Exception:
                log.exception("reaper pass failed")
            await sleep_or_stop(stop, self._settings.reaper_interval)

    async def run_once(self) -> list[Transition]:
        async with self._engine.begin() as conn:
            reaped = await jobs_repo.reap_expired_leases(
                conn,
                limit=self._settings.reaper_batch_size,
                default_callback_url=self._settings.default_callback_url,
            )
            await jobs_repo.delete_old_rate_limit_buckets(
                conn, window_seconds=self._settings.rate_limit_window_seconds
            )
        for transition in reaped:
            log.warning(
                "reclaimed job %s after lease expiry -> %s",
                transition.job_id,
                transition.status.value,
            )
            self._invalidator.invalidate(transition)
        return reaped
