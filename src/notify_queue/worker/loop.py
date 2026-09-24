"""The delivery worker: claim due jobs, rate-limit, send, finalize.

Many instances run at once (processes, containers, and several loops per process).
They coordinate only through Postgres:

  claim     one short transaction: FOR UPDATE SKIP LOCKED + status flip + fresh
            claim_token. Committed before any network I/O.
  gate      per-recipient rate limit (Redis sliding window, Postgres fallback);
            over the limit -> back to the queue until a slot frees up (not a
            failure, no attempt used).
  budget    only send if the lease still has room for the send, a wait for a
            connection to record the result, and a margin; otherwise put the job
            back unsent (no attempt used), so a send never outlives its claim.
  send      outside any transaction, bounded by send_timeout.
  finalize  one transaction fenced on claim_token: status + delivery ledger +
            attempt record + webhook outbox event commit together, or not at all.
"""

import asyncio
import logging
import random
import time
from collections import Counter

from sqlalchemy.ext.asyncio import AsyncEngine

from notify_queue.config import Settings
from notify_queue.domain.backoff import backoff_delay
from notify_queue.domain.models import Job
from notify_queue.repositories import jobs as jobs_repo
from notify_queue.senders.base import DeliveryError, PermanentDeliveryError, Sender
from notify_queue.services.rate_limiter import RateLimiter
from notify_queue.worker.common import CacheInvalidator, sleep_or_stop

log = logging.getLogger(__name__)


class Worker:
    def __init__(
        self,
        engine: AsyncEngine,
        sender: Sender,
        rate_limiter: RateLimiter,
        settings: Settings,
        invalidator: CacheInvalidator,
        worker_id: str | None = None,
    ) -> None:
        self._engine = engine
        self._sender = sender
        self._rate_limiter = rate_limiter
        self._settings = settings
        self._invalidator = invalidator
        self.worker_id = worker_id or settings.worker_id
        self.stats: Counter[str] = Counter()
        self.last_activity = time.monotonic()

    async def run(self, stop: asyncio.Event) -> None:
        """Run ``worker_concurrency`` claim loops until ``stop`` is set. In-flight
        batches finish before returning (graceful shutdown)."""
        loops = [
            asyncio.create_task(self._loop(stop, f"{self.worker_id}/{i}"))
            for i in range(self._settings.worker_concurrency)
        ]
        await asyncio.gather(*loops)
        await self._invalidator.drain()

    async def _loop(self, stop: asyncio.Event, loop_id: str) -> None:
        while not stop.is_set():
            try:
                claimed = await self.run_batch(loop_id)
            except Exception:
                log.exception("claim loop %s failed; backing off", loop_id)
                claimed = 0
            if claimed == 0:
                # Jitter keeps idle workers from polling in lockstep.
                await sleep_or_stop(stop, self._settings.poll_interval * random.uniform(0.5, 1.5))

    async def run_batch(self, loop_id: str | None = None) -> int:
        """Claim and process one batch. Returns how many jobs were claimed."""
        # Taken before the claim query is sent, so it is no later than the database's
        # now() that starts the lease: this deadline can only be early, never late,
        # whatever the clock difference between worker and database.
        lease_deadline = time.monotonic() + self._settings.lease_seconds
        async with self._engine.begin() as conn:
            jobs = await jobs_repo.claim_due_jobs(
                conn,
                worker_id=loop_id or self.worker_id,
                batch_size=self._settings.batch_size,
                lease_seconds=self._settings.lease_seconds,
            )
        if jobs:
            self.last_activity = time.monotonic()
            await asyncio.gather(*(self._process(job, lease_deadline) for job in jobs))
        return len(jobs)

    async def _process(self, job: Job, lease_deadline: float) -> None:
        try:
            await self._deliver(job, lease_deadline)
        except Exception:
            # Never let one job kill the loop. The job stays 'processing'; its lease
            # expires and the reaper returns it to the queue as a failed attempt.
            self.stats["errors"] += 1
            log.exception("unexpected error processing job %s", job.id)

    async def _deliver(self, job: Job, lease_deadline: float) -> None:
        assert job.claim_token is not None
        worker_id = job.locked_by or self.worker_id

        retry_after = await self._rate_limiter.acquire(job)
        if retry_after is not None:
            await self._return_unsent(job, delay_seconds=retry_after)
            self.stats["rate_limited"] += 1
            return

        if lease_deadline - time.monotonic() < self._settings.send_budget_seconds:
            # Waiting (for a connection, or for the rate limiter) used up too much of
            # the lease. Sending now could outlive the claim, letting another worker
            # send the same job. Put it back unsent and give back its slot.
            returned = await self._return_unsent(job, delay_seconds=0)
            if returned:
                await self._rate_limiter.release(job)
            self.stats["lease_budget_exceeded"] += 1
            log.warning("job %s returned unsent: too little lease left to send safely", job.id)
            return

        error: str | None = None
        permanent = False
        refund_slot = False
        provider_message_id: str | None = None
        try:
            result = await asyncio.wait_for(
                self._sender.send(job), timeout=self._settings.send_timeout_seconds
            )
            provider_message_id = result.provider_message_id
            if result.deduplicated:
                self.stats["provider_deduplicated"] += 1
        except PermanentDeliveryError as exc:
            error, permanent, refund_slot = f"PermanentDeliveryError: {exc}", True, True
        except DeliveryError as exc:
            # The provider rejected the send, so nothing was delivered: the rate-limit
            # slot can be refunded.
            error, refund_slot = f"DeliveryError: {exc}", True
        except Exception as exc:
            # Timeouts and unexpected errors: the message may still have gone out, so
            # the slot is kept (never risk sending more than the limit).
            error = f"{type(exc).__name__}: {exc}"

        async with self._engine.begin() as conn:
            if error is None:
                transition = await jobs_repo.mark_sent(
                    conn,
                    job_id=job.id,
                    claim_token=job.claim_token,
                    worker_id=worker_id,
                    provider_message_id=provider_message_id,
                    default_callback_url=self._settings.default_callback_url,
                )
            else:
                transition = await jobs_repo.mark_failed(
                    conn,
                    job_id=job.id,
                    claim_token=job.claim_token,
                    worker_id=worker_id,
                    error=error,
                    retry_delay=backoff_delay(
                        job.attempts,
                        base=self._settings.backoff_base_seconds,
                        cap=self._settings.backoff_cap_seconds,
                    ),
                    permanent=permanent,
                    default_callback_url=self._settings.default_callback_url,
                )

        if transition is None:
            # Our lease expired and the job was handed to someone else. The provider
            # idempotency key (job.id) absorbs any resend by the new owner.
            self.stats["lease_lost"] += 1
            log.warning("lost lease on job %s before finalizing; result discarded", job.id)
            return

        if refund_slot:
            # Only after recording the result while still owning the job. If the job
            # had been reclaimed, the new owner shares this slot (same job id) and
            # releasing it could let one extra message through.
            await self._rate_limiter.release(job)
        self.stats[transition.status.value] += 1
        if error is not None:
            self.stats["failed_attempts"] += 1
            log.info(
                "job %s attempt %d failed (%s) -> %s",
                job.id,
                job.attempts,
                error,
                transition.status.value,
            )
        self._invalidator.invalidate(transition)

    async def _return_unsent(self, job: Job, *, delay_seconds: float) -> bool:
        """Put a claimed, unsent job back in the queue. Returns False if this worker
        no longer owns it (its lease expired and it was reclaimed)."""
        assert job.claim_token is not None
        async with self._engine.begin() as conn:
            transition = await jobs_repo.return_to_queue(
                conn, job_id=job.id, claim_token=job.claim_token, delay_seconds=delay_seconds
            )
        if transition is None:
            return False
        self._invalidator.invalidate(transition)
        return True
