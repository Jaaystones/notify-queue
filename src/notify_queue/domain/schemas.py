"""API request/response models."""

import hashlib
import json
from datetime import datetime
from typing import Annotated, Any
from uuid import UUID

from pydantic import (
    AwareDatetime,
    BaseModel,
    BeforeValidator,
    Field,
    HttpUrl,
    PlainSerializer,
    model_validator,
)

from notify_queue.domain.enums import Channel, JobStatus, Priority
from notify_queue.domain.models import Job, JobAttempt

PriorityField = Annotated[
    Priority,
    BeforeValidator(Priority.parse),
    PlainSerializer(lambda p: p.name.lower(), return_type=str),
]

MAX_DELAY_SECONDS = 365 * 24 * 3600


class ScheduleJobRequest(BaseModel):
    recipient: str = Field(min_length=1, max_length=320)
    channel: Channel
    payload: dict[str, Any]
    priority: PriorityField = Priority.NORMAL
    send_at: AwareDatetime | None = None
    delay_seconds: float | None = Field(default=None, ge=0, le=MAX_DELAY_SECONDS)
    max_attempts: int | None = Field(default=None, ge=1, le=20)
    callback_url: HttpUrl | None = None
    idempotency_key: str | None = Field(default=None, min_length=1, max_length=255)

    @model_validator(mode="after")
    def _one_schedule_mode(self) -> "ScheduleJobRequest":
        if self.send_at is not None and self.delay_seconds is not None:
            raise ValueError("give either send_at or delay_seconds, not both")
        return self

    def request_hash(self) -> str:
        """Fingerprint of the request body, used to detect idempotency-key reuse
        with a different request."""
        body = self.model_dump(mode="json", exclude={"idempotency_key"})
        canonical = json.dumps(body, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(canonical.encode()).hexdigest()


class JobAttemptOut(BaseModel):
    attempt_no: int
    worker_id: str | None
    outcome: str
    error: str | None
    started_at: datetime | None
    finished_at: datetime

    @classmethod
    def from_domain(cls, attempt: JobAttempt) -> "JobAttemptOut":
        return cls.model_validate(attempt, from_attributes=True)


class JobOut(BaseModel):
    id: UUID
    idempotency_key: str | None
    recipient: str
    channel: Channel
    payload: dict[str, Any]
    priority: PriorityField
    status: JobStatus
    retrying: bool
    run_at: datetime
    attempts: int
    max_attempts: int
    last_error: str | None
    callback_url: str | None
    version: int
    created_at: datetime
    updated_at: datetime
    sent_at: datetime | None
    attempt_history: list[JobAttemptOut] | None = None

    @classmethod
    def from_domain(cls, job: Job) -> "JobOut":
        return cls(
            id=job.id,
            idempotency_key=job.idempotency_key,
            recipient=job.recipient,
            channel=job.channel,
            payload=job.payload,
            priority=job.priority,
            status=job.status,
            retrying=job.is_retrying,
            run_at=job.run_at,
            attempts=job.attempts,
            max_attempts=job.max_attempts,
            last_error=job.last_error,
            callback_url=job.callback_url,
            version=job.version,
            created_at=job.created_at,
            updated_at=job.updated_at,
            sent_at=job.sent_at,
        )


class ErrorOut(BaseModel):
    detail: str
