import asyncio
import logging

from notify_queue.cache.job_cache import JobCache
from notify_queue.repositories.jobs import Transition

log = logging.getLogger(__name__)


async def sleep_or_stop(stop: asyncio.Event, seconds: float) -> None:
    """Sleep for ``seconds``, waking early if ``stop`` is set."""
    try:
        await asyncio.wait_for(stop.wait(), timeout=seconds)
    except TimeoutError:
        pass


class CacheInvalidator:
    """Invalidates job cache entries after commits without blocking delivery.

    Invalidations run as background tasks: a remote Redis (Upstash) round trip is
    ~100s of ms, and correctness does not need them to finish first. Versioned
    tombstones make a late invalidation harmless, and the short TTL bounds the damage
    of a lost one.
    """

    def __init__(self, job_cache: JobCache | None) -> None:
        self._job_cache = job_cache
        self._tasks: set[asyncio.Task[None]] = set()

    def invalidate(self, transition: Transition) -> None:
        if self._job_cache is None:
            return
        task = asyncio.create_task(
            self._job_cache.invalidate(transition.job_id, transition.version)
        )
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def drain(self) -> None:
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
