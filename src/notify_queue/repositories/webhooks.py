"""SQL for the webhook outbox and the mock webhook receiver."""

from dataclasses import dataclass
from typing import Any
from uuid import UUID

from sqlalchemy import Boolean, Float, bindparam, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncConnection


@dataclass(frozen=True, slots=True)
class OutboxEvent:
    id: UUID
    job_id: UUID
    event: str
    payload: dict[str, Any]
    target_url: str
    attempts: int


_CLAIM_OUTBOX = text(
    """
    WITH due AS (
        SELECT id
        FROM webhook_outbox
        WHERE status = 'pending' AND next_attempt_at <= now()
        ORDER BY next_attempt_at
        LIMIT :limit
        FOR UPDATE SKIP LOCKED
    )
    UPDATE webhook_outbox AS o
    -- Pushing next_attempt_at out acts as a lease: other dispatchers skip this row
    -- while we POST it, and it becomes due again if this dispatcher dies.
    SET next_attempt_at = now() + make_interval(secs => :lease_seconds),
        attempts        = o.attempts + 1
    FROM due
    WHERE o.id = due.id
    RETURNING o.id, o.job_id, o.event, o.payload, o.target_url, o.attempts
    """
).bindparams(bindparam("lease_seconds", type_=Float))


async def claim_outbox_events(
    conn: AsyncConnection, *, limit: int, lease_seconds: float
) -> list[OutboxEvent]:
    result = await conn.execute(_CLAIM_OUTBOX, {"limit": limit, "lease_seconds": lease_seconds})
    return [OutboxEvent(**row) for row in result.mappings()]


async def mark_delivered(conn: AsyncConnection, event_id: UUID) -> None:
    await conn.execute(
        text(
            """
            UPDATE webhook_outbox
            SET status = 'delivered', delivered_at = now(), last_error = NULL
            WHERE id = :id
            """
        ),
        {"id": event_id},
    )


async def mark_attempt_failed(
    conn: AsyncConnection, event_id: UUID, *, error: str, retry_delay: float | None
) -> None:
    """Schedule another try in ``retry_delay`` seconds, or give up if it is None."""
    await conn.execute(
        text(
            """
            UPDATE webhook_outbox
            SET status          = CASE WHEN :give_up THEN 'failed' ELSE 'pending' END,
                next_attempt_at = now() + make_interval(secs => :retry_delay),
                last_error      = :error
            WHERE id = :id
            """
        ).bindparams(bindparam("retry_delay", type_=Float), bindparam("give_up", type_=Boolean)),
        {
            "id": event_id,
            "error": error,
            "give_up": retry_delay is None,
            "retry_delay": retry_delay or 0.0,
        },
    )


async def record_receipt(conn: AsyncConnection, payload: dict[str, Any]) -> bool:
    """Store a webhook received by the mock endpoint. Returns False for a redelivery
    of an event already received (the outbox is at-least-once)."""
    result = await conn.execute(
        text(
            """
            INSERT INTO mock_webhook_receipts (event_id, job_id, event, payload)
            VALUES (:event_id, :job_id, :event, :payload)
            ON CONFLICT (event_id) DO NOTHING
            RETURNING id
            """
        ).bindparams(bindparam("payload", type_=JSONB)),
        {
            "event_id": UUID(payload["event_id"]),
            "job_id": UUID(payload["job_id"]),
            "event": payload["event"],
            "payload": payload,
        },
    )
    return result.first() is not None


async def list_receipts(
    conn: AsyncConnection, *, job_id: UUID | None, limit: int
) -> list[dict[str, Any]]:
    result = await conn.execute(
        text(
            """
            SELECT event_id, job_id, event, payload, received_at
            FROM mock_webhook_receipts
            WHERE CAST(:job_id AS uuid) IS NULL OR job_id = :job_id
            ORDER BY id DESC
            LIMIT :limit
            """
        ),
        {"job_id": job_id, "limit": limit},
    )
    return [dict(row) for row in result.mappings()]
