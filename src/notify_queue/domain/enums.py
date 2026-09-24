from enum import IntEnum, StrEnum


class Channel(StrEnum):
    EMAIL = "email"
    SMS = "sms"
    PUSH = "push"


class JobStatus(StrEnum):
    PENDING = "pending"
    PROCESSING = "processing"
    SENT = "sent"
    DEAD_LETTERED = "dead_lettered"

    @property
    def is_terminal(self) -> bool:
        return self in (JobStatus.SENT, JobStatus.DEAD_LETTERED)


class Priority(IntEnum):
    """Stored as a smallint; higher values are claimed first."""

    LOW = 0
    NORMAL = 1
    HIGH = 2
    CRITICAL = 3

    @classmethod
    def parse(cls, value: object) -> "Priority":
        if isinstance(value, cls):
            return value
        if isinstance(value, str):
            try:
                return cls[value.upper()]
            except KeyError:
                raise ValueError(
                    f"priority must be one of {[p.name.lower() for p in cls]}"
                ) from None
        if isinstance(value, int) and not isinstance(value, bool):
            return cls(value)
        raise ValueError("priority must be a name or an integer 0-3")


class WebhookEvent(StrEnum):
    SENT = "sent"
    FAILED = "failed"
    DEAD_LETTERED = "dead_lettered"
