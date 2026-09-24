import os
import socket
from functools import lru_cache
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


def _default_worker_id() -> str:
    return f"{socket.gethostname()}-{os.getpid()}"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "postgresql+asyncpg://notify:notify@localhost:5440/notify_queue"
    db_pool_size: int = 10
    db_max_overflow: int = 20

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
    lease_seconds: float = 30.0
    # Must stay well under lease_seconds so a slow send cannot outlive its claim.
    send_timeout_seconds: float = 10.0
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


@lru_cache
def get_settings() -> Settings:
    return Settings()
