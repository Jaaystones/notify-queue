"""All SQL touching the jobs table and its satellites.

Everything concurrency-critical lives in this module so it can be reviewed in one
place. Functions take an ``AsyncConnection`` that is already inside a transaction;
callers own the transaction boundaries (``async with engine.begin() as conn``).

Invariants:
  * A job is claimed by flipping ``pending -> processing`` under ``FOR UPDATE SKIP
    LOCKED`` and stamping a fresh ``claim_token``.
  * Every write after a claim is conditional on ``claim_token`` (fencing), so a
    worker whose lease expired cannot overwrite the job once someone else holds it.
  * Every update bumps ``version`` so the Redis cache can reject stale writes.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import Boolean, Float, Text, bindparam, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncConnection
from sqlalchemy.types import DateTime

from notify_queue.domain.enums import JobStatus, WebhookEvent
from notify_queue.domain.models import Job, JobAttempt, NewJob


@dataclass(frozen=True, slots=True)
class Transition:
    """A committed state change: which job moved and its new version. Callers use
    it to invalidate the cache after the transaction commits."""

    job_id: UUID
    version: int
    status: JobStatus


_INSERT_JOB = text(
    """
    INSERT INTO jobs (idempotency_key, request_hash, recipient, channel, payload,
                      priority, run_at, max_attempts, callback_url)
    VALUES (:idempotency_key, :request_hash, :recipient, :channel, :payload,
            :priority,
            COALESCE(:send_at, now() + make_interval(secs => COALESCE(:delay_seconds, 0))),
            :max_attempts, :callback_url)
    ON CONFLICT (idempotency_key) DO NOTHING
    RETURNING *
    """
).bindparams(
    bindparam("payload", type_=JSONB),
    bindparam("send_at", type_=DateTime(timezone=True)),
    bindparam("delay_seconds", type_=Float),
    bindparam("idempotency_key", type_=Text),
    bindparam("callback_url", type_=Text),
)


async def create_job(conn: AsyncConnection, new: NewJob) -> tuple[Job, bool]:
    """Insert a job, or return the existing one for a repeated idempotency key.

    ``ON CONFLICT DO NOTHING`` makes concurrent submissions of the same key safe:
    the losing insert waits for the winner to commit, returns no row, and the
    follow-up SELECT (a fresh READ COMMITTED snapshot) sees the winner's row.
    Returns ``(job, created)``.
    """
    result = await conn.execute(
        _INSERT_JOB,
        {
            "idempotency_key": new.idempotency_key,
            "request_hash": new.request_hash,
            "recipient": new.recipient,
            "channel": new.channel.value,
            "payload": new.payload,
            "priority": int(new.priority),
            "send_at": new.send_at,
            "delay_seconds": new.delay_seconds,
            "max_attempts": new.max_attempts,
            "callback_url": new.callback_url,
        },
    )
    row = result.mappings().first()
    if row is not None:
        return Job.from_row(row), True

    existing = await get_job_by_idempotency_key(conn, new.idempotency_key)
    if existing is None:  # pragma: no cover - only reachable if the row was deleted
        raise RuntimeError("idempotency conflict but no existing job found")
    return existing, False


async def get_job(conn: AsyncConnection, job_id: UUID) -> Job | None:
    result = await conn.execute(text("SELECT * FROM jobs WHERE id = :id"), {"id": job_id})
    row = result.mappings().first()
    return Job.from_row(row) if row else None


async def get_job_by_idempotency_key(conn: AsyncConnection, key: str | None) -> Job | None:
    if key is None:
        return None
    result = await conn.execute(
        text("SELECT * FROM jobs WHERE idempotency_key = :key"), {"key": key}
    )
    row = result.mappings().first()
    return Job.from_row(row) if row else None


async def get_job_attempts(conn: AsyncConnection, job_id: UUID) -> list[JobAttempt]:
    result = await conn.execute(
        text(
            """
            SELECT attempt_no, worker_id, outcome, error, started_at, finished_at
            FROM job_attempts WHERE job_id = :id ORDER BY attempt_no
            """
        ),
        {"id": job_id},
    )
    return [JobAttempt(**row) for row in result.mappings()]


_CLAIM_DUE_JOBS = text(
    """
    WITH due AS (
        SELECT id
        FROM jobs
        WHERE status = 'pending' AND run_at <= now()
        ORDER BY priority DESC, run_at ASC
        LIMIT :batch_size
        FOR UPDATE SKIP LOCKED
    )
    UPDATE jobs AS j
    SET status           = 'processing',
        claim_token      = gen_random_uuid(),
        locked_by        = :worker_id,
        claimed_at       = now(),
        lease_expires_at = now() + make_interval(secs => :lease_seconds),
        attempts         = j.attempts + 1,
        version          = j.version + 1,
        updated_at       = now()
    FROM due
    WHERE j.id = due.id
    RETURNING j.*
    """
).bindparams(bindparam("lease_seconds", type_=Float))


async def claim_due_jobs(
    conn: AsyncConnection, *, worker_id: str, batch_size: int, lease_seconds: float
) -> list[Job]:
    """Atomically claim up to ``batch_size`` due jobs, highest priority first.

    ``SKIP LOCKED`` means concurrent workers never block on, or double-claim, the
    same rows: each row is locked by exactly one claimer's transaction and the
    status flip to ``processing`` removes it from everyone else's due set. The
    caller should commit immediately so no row lock is held during delivery.
    """
    result = await conn.execute(
        _CLAIM_DUE_JOBS,
        {"worker_id": worker_id, "batch_size": batch_size, "lease_seconds": lease_seconds},
    )
    jobs = [Job.from_row(row) for row in result.mappings()]
    # UPDATE ... RETURNING does not preserve the CTE's order.
    jobs.sort(key=lambda job: (-job.priority, job.run_at))
    return jobs


_MARK_SENT = text(
    """
    UPDATE jobs
    SET status           = 'sent',
        sent_at          = now(),
        last_error       = NULL,
        claim_token      = NULL,
        locked_by        = NULL,
        lease_expires_at = NULL,
        version          = version + 1,
        updated_at       = now()
    WHERE id = :job_id AND claim_token = :claim_token AND status = 'processing'
    RETURNING id, version, status, attempts, claimed_at, recipient, channel, callback_url, sent_at
    """
)


async def mark_sent(
    conn: AsyncConnection,
    *,
    job_id: UUID,
    claim_token: UUID,
    worker_id: str,
    provider_message_id: str | None,
    default_callback_url: str,
) -> Transition | None:
    """Finalize a successful delivery.

    Fenced on ``claim_token``: returns ``None`` (and writes nothing) if this worker
    no longer owns the job, e.g. its lease expired and the job was reclaimed.
    The delivery ledger row, attempt record and ``sent`` webhook event are written
    in the same transaction as the status change.
    """
    result = await conn.execute(_MARK_SENT, {"job_id": job_id, "claim_token": claim_token})
    row = result.mappings().first()
    if row is None:
        return None

    await conn.execute(
        text(
            """
            INSERT INTO deliveries (job_id, worker_id, provider_message_id)
            VALUES (:job_id, :worker_id, :provider_message_id)
            """
        ),
        {"job_id": job_id, "worker_id": worker_id, "provider_message_id": provider_message_id},
    )
    await _record_attempt(
        conn,
        job_id=job_id,
        attempt_no=row["attempts"],
        worker_id=worker_id,
        outcome="sent",
        error=None,
        started_at=row["claimed_at"],
    )
    await enqueue_webhook(
        conn,
        job_id=job_id,
        event=WebhookEvent.SENT,
        target_url=row["callback_url"] or default_callback_url,
        data={
            "status": "sent",
            "recipient": row["recipient"],
            "channel": row["channel"],
            "attempts": row["attempts"],
            "sent_at": row["sent_at"].isoformat(),
        },
    )
    return _transition(row)


def _transition(row: Any) -> Transition:
    return Transition(job_id=row["id"], version=row["version"], status=JobStatus(row["status"]))


async def _record_attempt(
    conn: AsyncConnection,
    *,
    job_id: UUID,
    attempt_no: int,
    worker_id: str | None,
    outcome: str,
    error: str | None,
    started_at: datetime | None,
) -> None:
    await conn.execute(
        text(
            """
            INSERT INTO job_attempts (job_id, attempt_no, worker_id, outcome, error, started_at)
            VALUES (:job_id, :attempt_no, :worker_id, :outcome, :error, :started_at)
            """
        ).bindparams(bindparam("started_at", type_=DateTime(timezone=True))),
        {
            "job_id": job_id,
            "attempt_no": attempt_no,
            "worker_id": worker_id,
            "outcome": outcome,
            "error": error,
            "started_at": started_at,
        },
    )


async def enqueue_webhook(
    conn: AsyncConnection,
    *,
    job_id: UUID,
    event: WebhookEvent,
    target_url: str,
    data: dict[str, Any],
) -> UUID:
    """Add a status-change event to the transactional outbox."""
    event_id = uuid4()
    payload = {"event_id": str(event_id), "job_id": str(job_id), "event": event.value, **data}
    await conn.execute(
        text(
            """
            INSERT INTO webhook_outbox (id, job_id, event, payload, target_url)
            VALUES (:id, :job_id, :event, :payload, :target_url)
            """
        ).bindparams(bindparam("payload", type_=JSONB)),
        {
            "id": event_id,
            "job_id": job_id,
            "event": event.value,
            "payload": payload,
            "target_url": target_url,
        },
    )
    return event_id


_MARK_FAILED = text(
    """
    UPDATE jobs
    SET status           = CASE WHEN :permanent OR attempts >= max_attempts
                                THEN 'dead_lettered'::job_status
                                ELSE 'pending'::job_status END,
        run_at           = CASE WHEN :permanent OR attempts >= max_attempts
                                THEN run_at
                                ELSE now() + make_interval(secs => :retry_delay) END,
        last_error       = :error,
        claim_token      = NULL,
        locked_by        = NULL,
        lease_expires_at = NULL,
        version          = version + 1,
        updated_at       = now()
    WHERE id = :job_id AND claim_token = :claim_token AND status = 'processing'
    RETURNING id, version, status, attempts, max_attempts, claimed_at, recipient, channel,
              callback_url, run_at
    """
).bindparams(bindparam("retry_delay", type_=Float), bindparam("permanent", type_=Boolean))


async def mark_failed(
    conn: AsyncConnection,
    *,
    job_id: UUID,
    claim_token: UUID,
    worker_id: str,
    error: str,
    retry_delay: float,
    permanent: bool,
    default_callback_url: str,
) -> Transition | None:
    """Record a failed delivery attempt.

    Retries with ``retry_delay`` while attempts remain; dead-letters the job when the
    retry cap is reached or the error is permanent. Fenced on ``claim_token`` like
    ``mark_sent``. Returns ``None`` if this worker no longer owns the job.
    """
    result = await conn.execute(
        _MARK_FAILED,
        {
            "job_id": job_id,
            "claim_token": claim_token,
            "error": error,
            "retry_delay": retry_delay,
            "permanent": permanent,
        },
    )
    row = result.mappings().first()
    if row is None:
        return None

    await _record_attempt(
        conn,
        job_id=job_id,
        attempt_no=row["attempts"],
        worker_id=worker_id,
        outcome="failed",
        error=error,
        started_at=row["claimed_at"],
    )
    reason = "permanent error" if permanent else "max attempts exceeded"
    await _after_failure(
        conn, row, error=error, reason=reason, default_callback_url=default_callback_url
    )
    return _transition(row)


async def _after_failure(
    conn: AsyncConnection, row: Any, *, error: str, reason: str, default_callback_url: str
) -> None:
    """Dead-letter bookkeeping and the matching webhook event for a failed attempt."""
    target_url = row["callback_url"] or default_callback_url
    common = {
        "recipient": row["recipient"],
        "channel": row["channel"],
        "attempts": row["attempts"],
        "max_attempts": row["max_attempts"],
        "error": error,
    }
    if row["status"] == JobStatus.DEAD_LETTERED:
        await conn.execute(
            text(
                """
                INSERT INTO dead_letters (job_id, reason, attempts, last_error)
                VALUES (:job_id, :reason, :attempts, :error)
                """
            ),
            {"job_id": row["id"], "reason": reason, "attempts": row["attempts"], "error": error},
        )
        await enqueue_webhook(
            conn,
            job_id=row["id"],
            event=WebhookEvent.DEAD_LETTERED,
            target_url=target_url,
            data={"status": "dead_lettered", "reason": reason, **common},
        )
    else:
        await enqueue_webhook(
            conn,
            job_id=row["id"],
            event=WebhookEvent.FAILED,
            target_url=target_url,
            data={"status": "retrying", "next_attempt_at": row["run_at"].isoformat(), **common},
        )


_ACQUIRE_RATE_LIMIT = text(
    """
    WITH win AS (
        SELECT to_timestamp(floor(extract(epoch FROM now()) / :window) * :window) AS start
    )
    INSERT INTO rate_limit_buckets AS b (recipient, window_start, count)
    SELECT :recipient, win.start, 1 FROM win
    ON CONFLICT (recipient, window_start)
        DO UPDATE SET count = b.count + 1
        WHERE b.count < :limit
    RETURNING count
    """
).bindparams(bindparam("window", type_=Float))


async def try_acquire_rate_limit(
    conn: AsyncConnection, *, recipient: str, limit: int, window_seconds: float
) -> float | None:
    """Take one send from ``recipient``'s allowance for the current fixed window.

    This is the Postgres limiter, used when Redis is unavailable or disabled. A
    single atomic upsert: the conditional ``DO UPDATE ... WHERE count < limit``
    means two workers can never both take the last slot. Returns ``None`` if the
    send is allowed, otherwise the seconds until the next window starts.
    """
    result = await conn.execute(
        _ACQUIRE_RATE_LIMIT,
        {"recipient": recipient, "limit": limit, "window": window_seconds},
    )
    if result.first() is not None:
        return None
    return await conn.scalar(
        text(
            """
            SELECT (floor(extract(epoch FROM now()) / :window) + 1) * :window
                   - extract(epoch FROM now())
            """
        ).bindparams(bindparam("window", type_=Float)),
        {"window": window_seconds},
    )


async def defer_for_rate_limit(
    conn: AsyncConnection, *, job_id: UUID, claim_token: UUID, delay_seconds: float
) -> Transition | None:
    """Put a claimed job back in the queue for ``delay_seconds`` without counting an
    attempt: being rate limited is queueing, not failure."""
    result = await conn.execute(
        text(
            """
            UPDATE jobs
            SET status           = 'pending',
                run_at           = now() + make_interval(secs => :delay_seconds),
                attempts         = attempts - 1,
                claim_token      = NULL,
                locked_by        = NULL,
                claimed_at       = NULL,
                lease_expires_at = NULL,
                version          = version + 1,
                updated_at       = now()
            WHERE id = :job_id AND claim_token = :claim_token AND status = 'processing'
            RETURNING id, version, status
            """
        ).bindparams(bindparam("delay_seconds", type_=Float)),
        {"job_id": job_id, "claim_token": claim_token, "delay_seconds": delay_seconds},
    )
    row = result.mappings().first()
    return _transition(row) if row else None


_REAP_EXPIRED = text(
    """
    WITH expired AS (
        SELECT id, locked_by, claimed_at
        FROM jobs
        WHERE status = 'processing' AND lease_expires_at < now()
        ORDER BY lease_expires_at
        LIMIT :limit
        FOR UPDATE SKIP LOCKED
    )
    UPDATE jobs AS j
    SET status           = CASE WHEN j.attempts >= j.max_attempts
                                THEN 'dead_lettered'::job_status
                                ELSE 'pending'::job_status END,
        run_at           = now(),
        last_error       = :error,
        claim_token      = NULL,
        locked_by        = NULL,
        lease_expires_at = NULL,
        version          = j.version + 1,
        updated_at       = now()
    FROM expired
    WHERE j.id = expired.id
    RETURNING j.id, j.version, j.status, j.attempts, j.max_attempts, j.recipient, j.channel,
              j.callback_url, j.run_at, expired.locked_by, expired.claimed_at
    """
)

LEASE_EXPIRED_ERROR = "lease expired: worker crashed or exceeded its lease"


async def reap_expired_leases(
    conn: AsyncConnection, *, limit: int, default_callback_url: str
) -> list[Transition]:
    """Return jobs whose worker vanished (lease expired) to the queue.

    The expired attempt counts against ``max_attempts``, so a message that crashes
    every worker that touches it (a poison message) ends in the dead letter queue
    instead of cycling forever. Any worker still holding the old ``claim_token``
    is fenced out of finalizing.
    """
    result = await conn.execute(_REAP_EXPIRED, {"limit": limit, "error": LEASE_EXPIRED_ERROR})
    rows = result.mappings().all()
    for row in rows:
        await _record_attempt(
            conn,
            job_id=row["id"],
            attempt_no=row["attempts"],
            worker_id=row["locked_by"],
            outcome="lease_expired",
            error=LEASE_EXPIRED_ERROR,
            started_at=row["claimed_at"],
        )
        await _after_failure(
            conn,
            row,
            error=LEASE_EXPIRED_ERROR,
            reason="max attempts exceeded",
            default_callback_url=default_callback_url,
        )
    return [_transition(row) for row in rows]


async def delete_old_rate_limit_buckets(conn: AsyncConnection, *, window_seconds: float) -> int:
    result = await conn.execute(
        text(
            "DELETE FROM rate_limit_buckets "
            "WHERE window_start < now() - make_interval(secs => :keep)"
        ).bindparams(bindparam("keep", type_=Float)),
        {"keep": window_seconds * 2},
    )
    return result.rowcount


async def job_counts(conn: AsyncConnection) -> dict[str, int]:
    """One pass over ``jobs`` for the metrics endpoint."""
    result = await conn.execute(
        text(
            """
            SELECT
                count(*) FILTER (WHERE status = 'pending')                      AS pending,
                count(*) FILTER (WHERE status = 'pending' AND run_at <= now())  AS pending_due,
                count(*) FILTER (WHERE status = 'pending' AND run_at > now()
                                   AND last_error IS NULL)                      AS scheduled,
                count(*) FILTER (WHERE status = 'pending' AND last_error IS NOT NULL)
                                                                                AS retrying,
                count(*) FILTER (WHERE status = 'processing')                   AS processing,
                count(*) FILTER (WHERE status = 'sent')                         AS sent,
                count(*) FILTER (WHERE status = 'dead_lettered')                AS dead_lettered,
                (SELECT count(*) FROM job_attempts WHERE outcome <> 'sent')     AS failed_attempts,
                (SELECT count(*) FROM webhook_outbox WHERE status = 'pending')  AS webhooks_pending
            FROM jobs
            """
        )
    )
    return dict(result.mappings().one())


async def list_dead_letters(
    conn: AsyncConnection, *, limit: int, offset: int
) -> list[dict[str, Any]]:
    result = await conn.execute(
        text(
            """
            SELECT d.job_id, d.reason, d.attempts, d.last_error, d.dead_lettered_at,
                   j.recipient, j.channel, j.priority, j.payload
            FROM dead_letters d JOIN jobs j ON j.id = d.job_id
            ORDER BY d.dead_lettered_at DESC
            LIMIT :limit OFFSET :offset
            """
        ),
        {"limit": limit, "offset": offset},
    )
    return [dict(row) for row in result.mappings()]


async def requeue_dead_letter(
    conn: AsyncConnection, *, job_id: UUID, extra_attempts: int
) -> Transition | None:
    """Move a dead-lettered job back to the queue with ``extra_attempts`` more tries.

    ``attempts`` keeps counting (it is the job's full history), so the cap is raised
    instead of resetting the counter.
    """
    result = await conn.execute(
        text(
            """
            UPDATE jobs
            SET status       = 'pending',
                run_at       = now(),
                max_attempts = attempts + :extra_attempts,
                last_error   = NULL,
                version      = version + 1,
                updated_at   = now()
            WHERE id = :job_id AND status = 'dead_lettered'
            RETURNING id, version, status
            """
        ),
        {"job_id": job_id, "extra_attempts": extra_attempts},
    )
    row = result.mappings().first()
    if row is None:
        return None
    await conn.execute(text("DELETE FROM dead_letters WHERE job_id = :id"), {"id": job_id})
    return _transition(row)
