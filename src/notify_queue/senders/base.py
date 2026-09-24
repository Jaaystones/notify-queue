from dataclasses import dataclass
from typing import Protocol

from notify_queue.domain.models import Job


@dataclass(frozen=True, slots=True)
class SendResult:
    provider_message_id: str
    # True when the provider recognised the idempotency key and did not deliver again.
    deduplicated: bool = False


class DeliveryError(Exception):
    """A failed attempt that is worth retrying (timeouts, 5xx, throttling)."""


class PermanentDeliveryError(DeliveryError):
    """A failure retrying cannot fix (invalid recipient, rejected content). The job
    goes straight to the dead letter queue."""


class Sender(Protocol):
    async def send(self, job: Job) -> SendResult:
        """Deliver ``job``. Implementations must pass ``job.id`` to the provider as the
        idempotency key, so a re-send after a worker crash is not delivered twice."""
        ...
