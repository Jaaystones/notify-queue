"""Claim-level concurrency guarantees. The end-to-end no-duplicate-delivery test with
real workers and the mock sender arrives with the worker on Day 2."""

import asyncio
import uuid
from collections import Counter

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from notify_queue.domain.enums import JobStatus, Priority
from notify_queue.domain.models import Job
from notify_queue.repositories import jobs as jobs_repo
from tests.factories import insert_jobs


async def _claim(engine: AsyncEngine, worker_id: str, batch_size: int = 5) -> list[Job]:
    async with engine.begin() as conn:
        return await jobs_repo.claim_due_jobs(
            conn, worker_id=worker_id, batch_size=batch_size, lease_seconds=30
        )


async def _mark_sent(engine: AsyncEngine, job: Job, claim_token: uuid.UUID | None = None):
    async with engine.begin() as conn:
        return await jobs_repo.mark_sent(
            conn,
            job_id=job.id,
            claim_token=claim_token or job.claim_token,
            worker_id=job.locked_by or "test",
            provider_message_id="msg-1",
            default_callback_url="http://testserver/mock/webhooks",
        )


async def test_concurrent_claimers_never_claim_the_same_job(engine: AsyncEngine) -> None:
    await insert_jobs(engine, 300)
    claimed: list[uuid.UUID] = []

    async def claimer(worker_id: str) -> None:
        while batch := await _claim(engine, worker_id):
            claimed.extend(job.id for job in batch)

    await asyncio.gather(*(claimer(f"w{i}") for i in range(15)))

    duplicates = [job_id for job_id, n in Counter(claimed).items() if n > 1]
    assert duplicates == []
    assert len(claimed) == 300


async def test_claim_orders_by_priority_then_run_at(engine: AsyncEngine) -> None:
    await insert_jobs(engine, 2, priority=Priority.LOW)
    critical = await insert_jobs(engine, 1, priority=Priority.CRITICAL)
    high = await insert_jobs(engine, 2, priority=Priority.HIGH)

    batch = await _claim(engine, "w1", batch_size=3)

    assert [job.id for job in batch] == [critical[0].id, high[0].id, high[1].id]


async def test_future_jobs_are_not_claimed(engine: AsyncEngine) -> None:
    await insert_jobs(engine, 3, delay_seconds=3600)
    assert await _claim(engine, "w1") == []


async def test_claim_sets_lease_token_and_attempt(engine: AsyncEngine) -> None:
    [inserted] = await insert_jobs(engine, 1)

    [job] = await _claim(engine, "w1")

    assert job.status == JobStatus.PROCESSING
    assert job.claim_token is not None
    assert job.locked_by == "w1"
    assert job.attempts == 1
    assert job.version == inserted.version + 1
    assert job.lease_expires_at is not None


async def test_mark_sent_is_fenced_by_claim_token(engine: AsyncEngine) -> None:
    await insert_jobs(engine, 1)
    [job] = await _claim(engine, "w1")

    stale = await _mark_sent(engine, job, claim_token=uuid.uuid4())
    ok = await _mark_sent(engine, job)
    replay = await _mark_sent(engine, job)

    assert stale is None
    assert ok is not None and ok.version == job.version + 1
    assert replay is None
    async with engine.connect() as conn:
        deliveries = await conn.scalar(text("SELECT count(*) FROM deliveries"))
        events = (await conn.execute(text("SELECT event FROM webhook_outbox"))).scalars().all()
        stored = await jobs_repo.get_job(conn, job.id)
        attempts = await jobs_repo.get_job_attempts(conn, job.id)
    assert deliveries == 1
    assert events == ["sent"]
    assert stored is not None and stored.status == JobStatus.SENT
    assert [(a.attempt_no, a.outcome) for a in attempts] == [(1, "sent")]


async def test_stale_worker_cannot_finalize_after_reclaim(engine: AsyncEngine) -> None:
    await insert_jobs(engine, 1)
    [first] = await _claim(engine, "worker-a")
    # Simulate worker A's lease expiring and the job being handed back to the queue
    # (the reaper does this on Day 2).
    async with engine.begin() as conn:
        await conn.execute(
            text("UPDATE jobs SET status = 'pending', claim_token = NULL WHERE id = :id"),
            {"id": first.id},
        )
    [second] = await _claim(engine, "worker-b")

    assert await _mark_sent(engine, first) is None
    assert await _mark_sent(engine, second) is not None
