"""Stub provider for email/SMS/push.

It behaves like a real provider in the ways that matter to the design:
  * random latency and a configurable random failure rate;
  * dedupe on an idempotency key (``job.id``): a repeat request is absorbed and
    counted in ``duplicate_requests`` rather than delivered again;
  * its own storage (``mock_provider_log``), written in its own transaction,
    independent of the worker's job transaction.

Payload switches for demos and tests:
  ``{"simulate": "permanent_failure"}`` - always rejected as undeliverable;
  ``{"simulate": "poison"}`` - always raises an unexpected exception.
"""

import asyncio
import random

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from notify_queue.domain.models import Job
from notify_queue.senders.base import DeliveryError, PermanentDeliveryError, SendResult

_RECORD = text(
    """
    INSERT INTO mock_provider_log (provider_idempotency_key, job_id, channel, recipient)
    VALUES (:key, :job_id, :channel, :recipient)
    ON CONFLICT (provider_idempotency_key)
        DO UPDATE SET duplicate_requests = mock_provider_log.duplicate_requests + 1
    RETURNING id, duplicate_requests > 0 AS deduplicated
    """
)


class MockSender:
    def __init__(
        self,
        engine: AsyncEngine,
        *,
        failure_rate: float,
        latency_range: tuple[float, float] = (0.02, 0.1),
        rng: random.Random | None = None,
    ) -> None:
        self._engine = engine
        self._failure_rate = failure_rate
        self._latency_range = latency_range
        self._rng = rng or random.Random()

    async def send(self, job: Job) -> SendResult:
        await asyncio.sleep(self._rng.uniform(*self._latency_range))

        simulate = job.payload.get("simulate")
        if simulate == "poison":
            raise ValueError("unparseable payload")  # an unexpected, non-delivery error
        if simulate == "permanent_failure":
            raise PermanentDeliveryError(f"recipient {job.recipient} rejected by provider")
        if self._rng.random() < self._failure_rate:
            raise DeliveryError("simulated provider failure (503)")

        async with self._engine.begin() as conn:
            row = (
                await conn.execute(
                    _RECORD,
                    {
                        "key": str(job.id),
                        "job_id": job.id,
                        "channel": job.channel.value,
                        "recipient": job.recipient,
                    },
                )
            ).one()
        return SendResult(provider_message_id=f"mock-{row.id}", deduplicated=row.deduplicated)
