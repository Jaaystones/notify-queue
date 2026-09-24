"""Test fixtures. Tests run against real Postgres and Redis (``make up``), because
the behaviour under test (SKIP LOCKED, Lua scripts) cannot be faked faithfully.

Tests always use the local Redis container, never the Upstash URL in .env, so the
suite does not spend Upstash command quota.
"""

import os
import uuid
from collections.abc import AsyncIterator

import pytest
from alembic import command
from alembic.config import Config
from httpx import ASGITransport, AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from notify_queue.api.main import create_app
from notify_queue.cache.client import Cache
from notify_queue.cache.job_cache import JobCache
from notify_queue.config import Settings
from notify_queue.db.session import create_engine

TEST_DATABASE_URL = os.environ.get(
    "TEST_DATABASE_URL", "postgresql+asyncpg://notify:notify@localhost:5440/notify_queue_test"
)
TEST_REDIS_URL = os.environ.get("TEST_REDIS_URL", "redis://localhost:6390/15")

TABLES = (
    "mock_webhook_receipts, mock_provider_log, webhook_outbox, rate_limit_buckets, "
    "job_attempts, dead_letters, deliveries, jobs"
)


def make_settings(**overrides: object) -> Settings:
    values: dict[str, object] = {
        "database_url": TEST_DATABASE_URL,
        "redis_url": TEST_REDIS_URL,
        # A fresh prefix per test keeps tests isolated without flushing Redis.
        "redis_key_prefix": f"nqtest:{uuid.uuid4().hex[:8]}:",
        "default_callback_url": "http://testserver/mock/webhooks",
    }
    values.update(overrides)
    return Settings(_env_file=None, **values)


@pytest.fixture(scope="session", autouse=True)
def _migrate() -> None:
    cfg = Config("alembic.ini")
    cfg.set_main_option("sqlalchemy.url", TEST_DATABASE_URL)
    cfg.attributes["configure_logger"] = False
    command.upgrade(cfg, "head")


@pytest.fixture
def settings() -> Settings:
    return make_settings()


@pytest.fixture
async def engine(settings: Settings) -> AsyncIterator[AsyncEngine]:
    engine = create_engine(settings)
    async with engine.begin() as conn:
        await conn.execute(text(f"TRUNCATE {TABLES} RESTART IDENTITY CASCADE"))
    yield engine
    await engine.dispose()


@pytest.fixture
async def cache(settings: Settings) -> AsyncIterator[Cache]:
    cache = Cache.from_settings(settings)
    yield cache
    await cache.close()


@pytest.fixture
def job_cache(cache: Cache, settings: Settings) -> JobCache:
    return JobCache(cache, settings)


@pytest.fixture
async def client(settings: Settings, engine: AsyncEngine) -> AsyncIterator[AsyncClient]:
    # Depends on ``engine`` so tables are truncated before the app starts.
    app = create_app(settings)
    async with app.router.lifespan_context(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://testserver") as client:
            yield client
