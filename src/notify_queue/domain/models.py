"""Row-level domain objects returned by the repository layer."""

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID

from notify_queue.domain.enums import Channel, JobStatus, Priority


@dataclass(frozen=True, slots=True)
class NewJob:
    recipient: str
    channel: Channel
    payload: dict[str, Any]
    priority: Priority
    max_attempts: int
    request_hash: str
    idempotency_key: str | None = None
    send_at: datetime | None = None
    delay_seconds: float | None = None
    callback_url: str | None = None


@dataclass(frozen=True, slots=True)
class Job:
    id: UUID
    idempotency_key: str | None
    request_hash: str
    recipient: str
    channel: Channel
    payload: dict[str, Any]
    priority: Priority
    status: JobStatus
    run_at: datetime
    attempts: int
    max_attempts: int
    last_error: str | None
    claim_token: UUID | None
    locked_by: str | None
    claimed_at: datetime | None
    lease_expires_at: datetime | None
    callback_url: str | None
    version: int
    created_at: datetime
    updated_at: datetime
    sent_at: datetime | None

    @classmethod
    def from_row(cls, row: Mapping[Any, Any]) -> "Job":
        # Mapping[Any, Any] rather than Mapping[str, Any]: SQLAlchemy's RowMapping
        # types its keys as str | Column, and Mapping's key type is invariant.
        # Fields are listed explicitly so each one is type-checked on its own.
        return cls(
            id=row["id"],
            idempotency_key=row["idempotency_key"],
            request_hash=row["request_hash"],
            recipient=row["recipient"],
            channel=Channel(row["channel"]),
            payload=row["payload"],
            priority=Priority(row["priority"]),
            status=JobStatus(row["status"]),
            run_at=row["run_at"],
            attempts=row["attempts"],
            max_attempts=row["max_attempts"],
            last_error=row["last_error"],
            claim_token=row["claim_token"],
            locked_by=row["locked_by"],
            claimed_at=row["claimed_at"],
            lease_expires_at=row["lease_expires_at"],
            callback_url=row["callback_url"],
            version=row["version"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            sent_at=row["sent_at"],
        )

    @property
    def is_retrying(self) -> bool:
        return self.status == JobStatus.PENDING and self.attempts > 0 and bool(self.last_error)


@dataclass(frozen=True, slots=True)
class JobAttempt:
    attempt_no: int
    worker_id: str | None
    outcome: str
    error: str | None
    started_at: datetime | None
    finished_at: datetime

    @classmethod
    def from_row(cls, row: Mapping[Any, Any]) -> "JobAttempt":
        return cls(
            attempt_no=row["attempt_no"],
            worker_id=row["worker_id"],
            outcome=row["outcome"],
            error=row["error"],
            started_at=row["started_at"],
            finished_at=row["finished_at"],
        )
