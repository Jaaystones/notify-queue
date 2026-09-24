import os
import socket
from functools import lru_cache
from typing import Literal, Self

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


def _default_worker_id() -> str:
    return f"{socket.gethostname()}-{os.getpid()}"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "postgresql+asyncpg://notify:notify@localhost:5440/notify_queue"
    db_pool_size: int = 10
    db_max_overflow: int = 20
    # How long to wait for a free connection. Part of the lease budget (see below).
    db_pool_timeout: float = 10.0

    # Redis: rate limiter and read cache. Fail-open: never decides duplicate sends.
    redis_url: str = "redis://localhost:6390/0"
    redis_key_prefix: str = "nq:"
    redis_socket_timeout: float = 1.0
    redis_connect_timeout: float = 3.0
    cache_enabled: bool = True
    cache_job_ttl_active: int = 30
    cache_job_ttl_terminal: int = 86_400
    cache_tombstone_ttl: int = 60
    cache_idempotency_ttl: int = 86_400
    cache_metrics_ttl: int = 2

    # Worker
    worker_id: str = Field(default_factory=_default_worker_id)
    worker_concurrency: int = 4
    batch_size: int = 10
    poll_interval: float = 0.5
    # Lease budget: a job is only sent if, at that moment, its lease has room for
    # the send, a wait for a connection to record the result, and a safety margin.
    # The validator below rejects settings where that budget cannot fit at all.
    lease_seconds: float = 30.0
    send_timeout_seconds: float = 10.0
    lease_safety_margin_seconds: float = 2.0
    reaper_interval: float = 5.0
    reaper_batch_size: int = 100

    # Delivery policy
    max_attempts: int = 5
    backoff_base_seconds: float = 2.0
    backoff_cap_seconds: float = 300.0
    failure_rate: float = 0.1
    rate_limit_per_hour: int = 10
    # Window length. One hour per the brief; shorten it for demos.
    rate_limit_window_seconds: float = 3600.0
    # "redis": sliding window in Redis, falling back to Postgres if Redis is down.
    # "postgres": fixed window in Postgres only.
    rate_limit_backend: Literal["redis", "postgres"] = "redis"

    # Mock provider
    mock_latency_min: float = 0.02
    mock_latency_max: float = 0.1

    # Webhooks
    webhook_batch_size: int = 20
    webhook_poll_interval: float = 1.0
    webhook_timeout_seconds: float = 5.0
    webhook_max_attempts: int = 8
    mock_webhook_failure_rate: float = 0.0

    default_callback_url: str = "http://localhost:8000/mock/webhooks"

    @property
    def send_budget_seconds(self) -> float:
        """Lease time that must remain before a send may start."""
        return self.send_timeout_seconds + self.db_pool_timeout + self.lease_safety_margin_seconds

    @model_validator(mode="after")
    def _lease_fits_send_budget(self) -> Self:
        if self.send_budget_seconds >= self.lease_seconds:
            raise ValueError(
                "lease_seconds must exceed send_timeout_seconds + db_pool_timeout + "
                f"lease_safety_margin_seconds ({self.send_budget_seconds}s), otherwise a job "
                "could outlive its lease between sending and recording the result"
            )
        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()
