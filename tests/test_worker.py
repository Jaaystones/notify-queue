import asyncio
from datetime import UTC, datetime, timedelta

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from notify_queue.cache.job_cache import JobCache
from notify_queue.domain.enums import JobStatus, Priority
from notify_queue.domain.schemas import JobOut
from notify_queue.repositories import jobs as jobs_repo
from notify_queue.senders.mock import MockSender
from notify_queue.worker.common import CacheInvalidator
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


async def test_rate_limit_defers_excess_jobs_without_failing_them(engine: AsyncEngine) -> None:
    limited = await insert_jobs(engine, 5, recipient="busy@example.com")
    await insert_jobs(engine, 1, recipient="quiet@example.com")
    settings = make_settings(failure_rate=0, rate_limit_per_hour=3, batch_size=10)

    # The five busy@ jobs are processed concurrently, so this also exercises the
    # atomicity of the counter: exactly three may win.
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
