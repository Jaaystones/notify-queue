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
    def from_row(cls, row: Mapping[str, Any]) -> "Job":
        return cls(
            **{
                **row,
                "channel": Channel(row["channel"]),
                "priority": Priority(row["priority"]),
                "status": JobStatus(row["status"]),
            }
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
