import random
from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from notify_queue.cache.client import Cache
from notify_queue.cache.job_cache import JobCache
from notify_queue.config import Settings
from notify_queue.domain.enums import Channel, Priority
from notify_queue.domain.models import Job, NewJob
from notify_queue.repositories import jobs as jobs_repo
from notify_queue.senders.mock import MockSender
from notify_queue.services.rate_limiter import build_rate_limiter
from notify_queue.worker.common import CacheInvalidator
from notify_queue.worker.loop import Worker


def new_job(**overrides: Any) -> NewJob:
    values: dict[str, Any] = {
        "recipient": "user@example.com",
        "channel": Channel.EMAIL,
        "payload": {"subject": "hi"},
        "priority": Priority.NORMAL,
        "max_attempts": 3,
        "request_hash": "test",
    }
    values.update(overrides)
    return NewJob(**values)


async def insert_jobs(engine: AsyncEngine, count: int, **overrides: Any) -> list[Job]:
    async with engine.begin() as conn:
        return [(await jobs_repo.create_job(conn, new_job(**overrides)))[0] for _ in range(count)]


def job_json(**overrides: Any) -> dict[str, Any]:
    body: dict[str, Any] = {
        "recipient": "user@example.com",
        "channel": "email",
        "payload": {"subject": "Welcome", "body": "Hello"},
        "priority": "normal",
    }
    body.update(overrides)
    return body


def iso(dt: datetime) -> str:
    return dt.isoformat()


def build_worker(
    engine: AsyncEngine,
    settings: Settings,
    *,
    job_cache: JobCache | None = None,
    worker_id: str = "worker-test",
    rng: random.Random | None = None,
) -> Worker:
    """A worker wired like production: rate limiting per ``settings.rate_limit_backend``
    (Redis by default, against the local test Redis)."""
    sender = MockSender(
        engine, failure_rate=settings.failure_rate, latency_range=(0.0, 0.002), rng=rng
    )
    cache = Cache.from_settings(settings)
    OPEN_CACHES.append(cache)
    return Worker(
        engine,
        sender,
        build_rate_limiter(engine, cache, settings),
        settings,
        CacheInvalidator(job_cache),
        worker_id=worker_id,
    )


# Caches created by build_worker; closed after each test by conftest.
OPEN_CACHES: list[Cache] = []


async def make_all_due(engine: AsyncEngine) -> None:
    """Skip the backoff wait so the next claim picks retries up immediately."""
    async with engine.begin() as conn:
        await conn.execute(text("UPDATE jobs SET run_at = now() WHERE status = 'pending'"))


async def fetch_job(engine: AsyncEngine, job_id: UUID) -> Job:
    async with engine.connect() as conn:
        job = await jobs_repo.get_job(conn, job_id)
    assert job is not None
    return job


async def scalar(engine: AsyncEngine, sql: str, **params: Any) -> Any:
    async with engine.connect() as conn:
        return await conn.scalar(text(sql), params)


async def column(engine: AsyncEngine, sql: str, **params: Any) -> list[Any]:
    async with engine.connect() as conn:
        return list((await conn.execute(text(sql), params)).scalars())
