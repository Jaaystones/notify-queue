"""Scheduling and job lookup: coordinates Postgres (source of truth) and the cache."""

from datetime import UTC, datetime, timedelta
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncEngine

from notify_queue.cache.job_cache import JobCache
from notify_queue.config import Settings
from notify_queue.domain.models import NewJob
from notify_queue.domain.schemas import JobAttemptOut, JobOut, ScheduleJobRequest
from notify_queue.repositories import jobs as jobs_repo

# Tolerate small client/server clock skew on send_at.
SEND_AT_GRACE = timedelta(seconds=5)


class InvalidSchedule(ValueError):
    pass


class IdempotencyConflict(Exception):
    def __init__(self, job_id: UUID) -> None:
        super().__init__(f"idempotency key already used for a different request (job {job_id})")
        self.job_id = job_id


class JobService:
    def __init__(self, engine: AsyncEngine, job_cache: JobCache, settings: Settings) -> None:
        self._engine = engine
        self._cache = job_cache
        self._settings = settings

    async def schedule(self, request: ScheduleJobRequest) -> tuple[JobOut, bool]:
        """Schedule a job. Returns ``(job, created)``; ``created`` is False when an
        earlier request with the same idempotency key already scheduled it."""
        if request.send_at is not None and request.send_at < datetime.now(UTC) - SEND_AT_GRACE:
            raise InvalidSchedule("send_at is in the past")

        request_hash = request.request_hash()
        key = request.idempotency_key

        # Fast path: a key we have seen recently. Postgres stays the authority, so a
        # miss here (or Redis being down) just falls through to the insert.
        if key is not None:
            cached = await self._cache.get_idempotency(key)
            if cached is not None:
                job_id, cached_hash = cached
                if cached_hash != request_hash:
                    raise IdempotencyConflict(job_id)
                existing = await self.get(job_id)
                if existing is not None:
                    return existing, False

        new = NewJob(
            recipient=request.recipient,
            channel=request.channel,
            payload=request.payload,
            priority=request.priority,
            max_attempts=request.max_attempts or self._settings.max_attempts,
            request_hash=request_hash,
            idempotency_key=key,
            send_at=request.send_at,
            delay_seconds=request.delay_seconds,
            callback_url=str(request.callback_url) if request.callback_url else None,
        )
        async with self._engine.begin() as conn:
            job, created = await jobs_repo.create_job(conn, new)

        if not created and job.request_hash != request_hash:
            raise IdempotencyConflict(job.id)

        if key is not None:
            await self._cache.put_idempotency(key, job.id, job.request_hash)
        out = JobOut.from_domain(job)
        await self._cache.put(out)
        return out, created

    async def get(self, job_id: UUID, *, include_attempts: bool = False) -> JobOut | None:
        if not include_attempts:
            cached = await self._cache.get(job_id)
            if cached is not None:
                return cached

        async with self._engine.connect() as conn:
            job = await jobs_repo.get_job(conn, job_id)
            if job is None:
                return None
            attempts = await jobs_repo.get_job_attempts(conn, job_id) if include_attempts else None

        out = JobOut.from_domain(job)
        await self._cache.put(out)
        if attempts is not None:
            out.attempt_history = [JobAttemptOut.from_domain(a) for a in attempts]
        return out
