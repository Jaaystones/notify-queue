import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from notify_queue.api.routes import dead_letters, health, jobs, metrics, mock
from notify_queue.cache.client import Cache
from notify_queue.cache.job_cache import JobCache
from notify_queue.cache.metrics_cache import MetricsCache
from notify_queue.config import Settings, get_settings
from notify_queue.db.session import create_engine
from notify_queue.services.dead_letters import DeadLetterService
from notify_queue.services.metrics import MetricsService
from notify_queue.services.scheduler import JobService


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        engine = create_engine(settings)
        cache = Cache.from_settings(settings)
        app.state.settings = settings
        app.state.engine = engine
        app.state.cache = cache
        job_cache = JobCache(cache, settings)
        app.state.job_service = JobService(engine, job_cache, settings)
        app.state.metrics_service = MetricsService(
            engine, MetricsCache(cache, settings.cache_metrics_ttl)
        )
        app.state.dead_letter_service = DeadLetterService(engine, job_cache)
        try:
            yield
        finally:
            await cache.close()
            await engine.dispose()

    app = FastAPI(title="Notify Queue", version="0.1.0", lifespan=lifespan)
    app.include_router(health.router)
    app.include_router(jobs.router)
    app.include_router(metrics.router)
    app.include_router(dead_letters.router)
    app.include_router(mock.router)
    return app


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
app = create_app()
