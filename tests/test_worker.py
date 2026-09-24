import asyncio
import uuid
from datetime import UTC, datetime, timedelta

import pytest
from pydantic import ValidationError
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from notify_queue.cache.job_cache import JobCache
from notify_queue.config import Settings
from notify_queue.domain.enums import JobStatus, Priority
from notify_queue.domain.models import Job
from notify_queue.domain.schemas import JobOut
from notify_queue.repositories import jobs as jobs_repo
from notify_queue.senders.mock import MockSender
from notify_queue.worker.common import CacheInvalidator
from notify_queue.worker.loop import Worker
from notify_queue.worker.reaper import Reaper
from tests.conftest import make_settings
from tests.factories import (
    build_worker,
    column,
    fetch_job,
    insert_jobs,
    make_all_due,
    scalar,
)


async def test_successful_delivery_records_everything_once(engine: AsyncEngine) -> None:
    [job] = await insert_jobs(engine, 1)
    worker = build_worker(engine, make_settings(failure_rate=0))

    assert await worker.run_batch() == 1

    stored = await fetch_job(engine, job.id)
    assert stored.status == JobStatus.SENT and stored.sent_at is not None
    assert await scalar(engine, "SELECT count(*) FROM deliveries") == 1
    assert await scalar(engine, "SELECT count(*) FROM mock_provider_log") == 1
    assert await column(engine, "SELECT event FROM webhook_outbox") == ["sent"]
    assert await column(engine, "SELECT outcome FROM job_attempts") == ["sent"]


async def test_retries_with_exponential_backoff_then_dead_letters(engine: AsyncEngine) -> None:
    [job] = await insert_jobs(engine, 1, max_attempts=3)
    settings = make_settings(failure_rate=1.0, backoff_base_seconds=10, backoff_cap_seconds=1000)
    worker = build_worker(engine, settings)

    for attempt in (1, 2):
        await worker.run_batch()
        stored = await fetch_job(engine, job.id)
        assert stored.status == JobStatus.PENDING and stored.attempts == attempt
        ceiling = 10 * 2 ** (attempt - 1)
        delay = (stored.run_at - datetime.now(UTC)).total_seconds()
        assert ceiling / 2 - 1 <= delay <= ceiling
        await make_all_due(engine)

    await worker.run_batch()

    stored = await fetch_job(engine, job.id)
    assert stored.status == JobStatus.DEAD_LETTERED and stored.attempts == 3
    assert await scalar(engine, "SELECT reason FROM dead_letters") == "max attempts exceeded"
    assert await column(engine, "SELECT event FROM webhook_outbox ORDER BY created_at") == [
        "failed",
        "failed",
        "dead_lettered",
    ]
    assert (
        await column(engine, "SELECT outcome FROM job_attempts ORDER BY attempt_no")
        == ["failed"] * 3
    )
    # Dead-lettered jobs are never claimed again.
    await make_all_due(engine)
    assert await worker.run_batch() == 0


async def test_permanent_failure_dead_letters_immediately(engine: AsyncEngine) -> None:
    [job] = await insert_jobs(engine, 1, payload={"simulate": "permanent_failure"})

    await build_worker(engine, make_settings(failure_rate=0)).run_batch()

    stored = await fetch_job(engine, job.id)
    assert stored.status == JobStatus.DEAD_LETTERED and stored.attempts == 1
    assert await scalar(engine, "SELECT reason FROM dead_letters") == "permanent error"


async def test_poison_message_is_retried_then_dead_lettered(engine: AsyncEngine) -> None:
    [job] = await insert_jobs(engine, 1, payload={"simulate": "poison"}, max_attempts=2)
    worker = build_worker(engine, make_settings(failure_rate=0))

    await worker.run_batch()
    await make_all_due(engine)
    await worker.run_batch()

    stored = await fetch_job(engine, job.id)
    assert stored.status == JobStatus.DEAD_LETTERED
    assert stored.last_error is not None and stored.last_error.startswith("ValueError")
    assert worker.stats["errors"] == 0  # handled as a failed attempt, not a crash


@pytest.mark.parametrize("backend", ["redis", "postgres"])
async def test_rate_limit_defers_excess_jobs_without_failing_them(
    engine: AsyncEngine, backend: str
) -> None:
    limited = await insert_jobs(engine, 5, recipient="busy@example.com")
    await insert_jobs(engine, 1, recipient="quiet@example.com")
    settings = make_settings(
        failure_rate=0, rate_limit_per_hour=3, batch_size=10, rate_limit_backend=backend
    )

    # The five busy@ jobs are processed concurrently, so this also exercises the
    # atomicity of the limiter: exactly three may win.
    await build_worker(engine, settings).run_batch()

    sent = await column(engine, "SELECT recipient FROM jobs WHERE status = 'sent'")
    assert sorted(sent) == ["busy@example.com"] * 3 + ["quiet@example.com"]
    deferred = [await fetch_job(engine, job.id) for job in limited]
    deferred = [job for job in deferred if job.status == JobStatus.PENDING]
    assert len(deferred) == 2
    for job in deferred:
        assert job.attempts == 0 and job.last_error is None
        assert job.run_at > datetime.now(UTC)
    assert await scalar(engine, "SELECT count(*) FROM webhook_outbox WHERE event <> 'sent'") == 0


async def test_higher_priority_jobs_are_sent_first(engine: AsyncEngine) -> None:
    await insert_jobs(engine, 1, priority=Priority.LOW, recipient="low")
    await insert_jobs(engine, 1, priority=Priority.NORMAL, recipient="normal")
    await insert_jobs(engine, 1, priority=Priority.CRITICAL, recipient="critical")
    await insert_jobs(engine, 1, priority=Priority.HIGH, recipient="high")
    worker = build_worker(engine, make_settings(failure_rate=0, batch_size=1))

    while await worker.run_batch():
        pass

    order = await column(engine, "SELECT recipient FROM mock_provider_log ORDER BY id")
    assert order == ["critical", "high", "normal", "low"]


async def _claim(engine: AsyncEngine, worker_id: str):
    async with engine.begin() as conn:
        [job] = await jobs_repo.claim_due_jobs(
            conn, worker_id=worker_id, batch_size=1, lease_seconds=30
        )
    return job


async def _expire_leases(engine: AsyncEngine) -> None:
    async with engine.begin() as conn:
        await conn.execute(text("UPDATE jobs SET lease_expires_at = now() - interval '1 second'"))


async def test_reaper_returns_abandoned_job_to_queue(engine: AsyncEngine) -> None:
    await insert_jobs(engine, 1)
    settings = make_settings()
    job = await _claim(engine, "crashed-worker")
    await _expire_leases(engine)

    [transition] = await Reaper(engine, settings, CacheInvalidator(None)).run_once()

    assert transition.status == JobStatus.PENDING
    stored = await fetch_job(engine, job.id)
    assert stored.attempts == 1 and stored.claim_token is None
    assert await column(engine, "SELECT outcome FROM job_attempts") == ["lease_expired"]
    assert await column(engine, "SELECT worker_id FROM job_attempts") == ["crashed-worker"]


async def test_reaper_dead_letters_job_that_keeps_crashing_workers(engine: AsyncEngine) -> None:
    await insert_jobs(engine, 1, max_attempts=1)
    await _claim(engine, "crashed-worker")
    await _expire_leases(engine)

    [transition] = await Reaper(engine, make_settings(), CacheInvalidator(None)).run_once()

    assert transition.status == JobStatus.DEAD_LETTERED
    assert await scalar(engine, "SELECT count(*) FROM dead_letters") == 1


async def test_crash_after_send_is_not_delivered_twice(engine: AsyncEngine) -> None:
    """The one window no queue can close by itself: worker A's send reached the
    provider, then A stalled before finalizing. The job is reclaimed and worker B
    sends again. The provider idempotency key (job.id) absorbs B's send."""
    await insert_jobs(engine, 1)
    settings = make_settings(failure_rate=0)
    job_a = await _claim(engine, "worker-a")
    await MockSender(engine, failure_rate=0).send(job_a)  # reached the provider...
    await _expire_leases(engine)  # ...then worker A stalled
    await Reaper(engine, settings, CacheInvalidator(None)).run_once()

    worker_b = build_worker(engine, settings, worker_id="worker-b")
    await worker_b.run_batch()

    async with engine.begin() as conn:
        late_a = await jobs_repo.mark_sent(
            conn,
            job_id=job_a.id,
            claim_token=job_a.claim_token,
            worker_id="worker-a",
            provider_message_id=None,
            default_callback_url="http://x",
        )
    assert late_a is None  # fenced out
    assert (await fetch_job(engine, job_a.id)).status == JobStatus.SENT
    assert worker_b.stats["provider_deduplicated"] == 1
    assert await scalar(engine, "SELECT count(*) FROM mock_provider_log") == 1
    assert await scalar(engine, "SELECT duplicate_requests FROM mock_provider_log") == 1
    assert await scalar(engine, "SELECT count(*) FROM deliveries") == 1


async def test_worker_invalidates_cached_status(engine: AsyncEngine, job_cache: JobCache) -> None:
    [job] = await insert_jobs(engine, 1)
    await job_cache.put(JobOut.from_domain(job))
    worker = build_worker(engine, make_settings(failure_rate=0), job_cache=job_cache)

    await worker.run_batch()
    await worker._invalidator.drain()

    assert await job_cache.get(job.id) is None  # tombstoned; next read goes to Postgres
    stale = JobOut.from_domain(job)
    assert not await job_cache.put(stale)


async def test_deferred_job_is_retried_in_next_window(engine: AsyncEngine) -> None:
    await insert_jobs(engine, 2, recipient="busy@example.com")
    settings = make_settings(failure_rate=0, rate_limit_per_hour=1, rate_limit_window_seconds=1)
    worker = build_worker(engine, settings)
    # Start right after a window boundary so both jobs land in the same window.
    now = datetime.now(UTC)
    await asyncio.sleep(1 - now.microsecond / 1_000_000 + 0.02)

    await worker.run_batch()
    next_window = await scalar(engine, "SELECT max(run_at) FROM jobs WHERE status = 'pending'")
    assert next_window - datetime.now(UTC) <= timedelta(seconds=1)
    await asyncio.sleep((next_window - datetime.now(UTC)).total_seconds() + 0.05)
    await worker.run_batch()

    assert await scalar(engine, "SELECT count(*) FROM jobs WHERE status = 'sent'") == 2


async def test_rejected_send_refunds_its_rate_limit_slot(engine: AsyncEngine) -> None:
    first, second = await insert_jobs(engine, 2, recipient="busy@example.com", max_attempts=3)
    settings = make_settings(
        failure_rate=1.0, rate_limit_per_hour=1, batch_size=1, backoff_base_seconds=600
    )
    worker = build_worker(engine, settings)

    await worker.run_batch()  # first job takes the only slot, the provider rejects it
    await worker.run_batch()  # the refunded slot lets the second job try

    for job in (first, second):
        stored = await fetch_job(engine, job.id)
        assert stored.attempts == 1
        assert stored.last_error is not None and stored.last_error.startswith("DeliveryError")
    assert worker.stats["rate_limited"] == 0


class _SlowRateLimiter:
    """Admits every job, but only after ``delay`` seconds (a congested Redis or pool)."""

    def __init__(self, delay: float = 0.0, error: Exception | None = None) -> None:
        self.delay = delay
        self.error = error
        self.released: list[uuid.UUID] = []

    async def acquire(self, job: Job) -> float | None:
        await asyncio.sleep(self.delay)
        if self.error is not None:
            raise self.error
        return None

    async def release(self, job: Job) -> None:
        self.released.append(job.id)


def _worker_with(engine: AsyncEngine, settings: Settings, limiter: _SlowRateLimiter) -> Worker:
    sender = MockSender(engine, failure_rate=0, latency_range=(0.0, 0.001))
    return Worker(engine, sender, limiter, settings, CacheInvalidator(None), worker_id="w")


def test_settings_reject_a_lease_too_short_for_the_send_budget() -> None:
    with pytest.raises(ValidationError, match="lease_seconds must exceed"):
        make_settings(lease_seconds=20, send_timeout_seconds=10, db_pool_timeout=10)


async def test_job_is_returned_unsent_when_its_lease_is_nearly_used_up(
    engine: AsyncEngine,
) -> None:
    [job] = await insert_jobs(engine, 1)
    # Budget = 0.5 + 0.5 + 0.5 = 1.5s. The limiter takes 2s of the 3s lease, leaving ~1s.
    settings = make_settings(
        lease_seconds=3,
        send_timeout_seconds=0.5,
        db_pool_timeout=0.5,
        lease_safety_margin_seconds=0.5,
    )
    limiter = _SlowRateLimiter(delay=2.0)
    worker = _worker_with(engine, settings, limiter)

    await worker.run_batch()

    stored = await fetch_job(engine, job.id)
    assert stored.status == JobStatus.PENDING
    assert stored.attempts == 0 and stored.claim_token is None
    assert await scalar(engine, "SELECT count(*) FROM mock_provider_log") == 0
    assert limiter.released == [job.id]
    assert worker.stats["lease_budget_exceeded"] == 1


async def test_job_with_enough_lease_left_is_sent(engine: AsyncEngine) -> None:
    [job] = await insert_jobs(engine, 1)
    settings = make_settings(
        lease_seconds=3,
        send_timeout_seconds=0.5,
        db_pool_timeout=0.5,
        lease_safety_margin_seconds=0.5,
    )
    worker = _worker_with(engine, settings, _SlowRateLimiter(delay=0.5))

    await worker.run_batch()

    assert (await fetch_job(engine, job.id)).status == JobStatus.SENT


async def test_unexpected_error_leaves_job_for_the_reaper(engine: AsyncEngine) -> None:
    [job] = await insert_jobs(engine, 1)
    settings = make_settings()
    worker = _worker_with(engine, settings, _SlowRateLimiter(error=RuntimeError("boom")))

    await worker.run_batch()  # must not raise: one bad job never kills the loop

    assert worker.stats["errors"] == 1
    assert (await fetch_job(engine, job.id)).status == JobStatus.PROCESSING
    await _expire_leases(engine)
    [transition] = await Reaper(engine, settings, CacheInvalidator(None)).run_once()
    assert transition.status == JobStatus.PENDING
    assert await column(engine, "SELECT outcome FROM job_attempts") == ["lease_expired"]
