from typing import Any
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncEngine

from notify_queue.cache.job_cache import JobCache
from notify_queue.repositories import jobs as jobs_repo


class NotDeadLettered(Exception):
    pass


class DeadLetterService:
    def __init__(self, engine: AsyncEngine, job_cache: JobCache) -> None:
        self._engine = engine
        self._cache = job_cache

    async def list(self, *, limit: int, offset: int) -> list[dict[str, Any]]:
        async with self._engine.connect() as conn:
            return await jobs_repo.list_dead_letters(conn, limit=limit, offset=offset)

    async def requeue(self, job_id: UUID, *, extra_attempts: int) -> None:
        async with self._engine.begin() as conn:
            transition = await jobs_repo.requeue_dead_letter(
                conn, job_id=job_id, extra_attempts=extra_attempts
            )
        if transition is None:
            raise NotDeadLettered(job_id)
        await self._cache.invalidate(transition.job_id, transition.version)
