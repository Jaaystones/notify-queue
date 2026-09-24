"""Worker process entry point: ``python -m notify_queue.worker``.

One process runs WORKER_CONCURRENCY delivery loops, a reaper and a webhook
dispatcher. Start as many processes as you like; they coordinate through Postgres.
"""

import argparse
import asyncio
import logging
import signal
import time

import httpx

from notify_queue.cache.client import Cache
from notify_queue.cache.job_cache import JobCache
from notify_queue.config import Settings, get_settings
from notify_queue.db.session import create_engine
from notify_queue.senders.mock import MockSender
from notify_queue.services.rate_limiter import build_rate_limiter
from notify_queue.worker.common import CacheInvalidator, sleep_or_stop
from notify_queue.worker.loop import Worker
from notify_queue.worker.reaper import Reaper
from notify_queue.worker.webhook_dispatcher import WebhookDispatcher

log = logging.getLogger("notify_queue.worker")

STATS_INTERVAL = 10.0


async def run_worker(settings: Settings, *, exit_when_idle: float | None = None) -> Worker:
    engine = create_engine(settings)
    cache = Cache.from_settings(settings)
    invalidator = CacheInvalidator(JobCache(cache, settings) if cache.enabled else None)
    sender = MockSender(
        engine,
        failure_rate=settings.failure_rate,
        latency_range=(settings.mock_latency_min, settings.mock_latency_max),
    )
    rate_limiter = build_rate_limiter(engine, cache, settings)
    worker = Worker(engine, sender, rate_limiter, settings, invalidator)
    stop = asyncio.Event()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    async def report() -> None:
        while not stop.is_set():
            await sleep_or_stop(stop, STATS_INTERVAL)
            log.info("worker %s stats: %s", worker.worker_id, dict(worker.stats))

    async def idle_watch(idle_seconds: float) -> None:
        while not stop.is_set():
            if time.monotonic() - worker.last_activity > idle_seconds:
                log.info("no work for %.0fs, exiting", idle_seconds)
                stop.set()
            await sleep_or_stop(stop, 0.5)

    log.info(
        "worker %s starting (%d loops, failure_rate=%.2f, rate limiter=%s)",
        worker.worker_id,
        settings.worker_concurrency,
        settings.failure_rate,
        type(rate_limiter).__name__,
    )
    async with httpx.AsyncClient() as http:
        tasks = [
            worker.run(stop),
            Reaper(engine, settings, invalidator).run(stop),
            WebhookDispatcher(engine, http, settings).run(stop),
            report(),
        ]
        if exit_when_idle is not None:
            tasks.append(idle_watch(exit_when_idle))
        try:
            await asyncio.gather(*tasks)
        finally:
            await invalidator.drain()
            await cache.close()
            await engine.dispose()
    log.info("worker %s stopped: %s", worker.worker_id, dict(worker.stats))
    return worker


def main() -> None:
    parser = argparse.ArgumentParser(description="Notify Queue delivery worker")
    parser.add_argument("--concurrency", type=int, help="delivery loops in this process")
    parser.add_argument(
        "--exit-when-idle",
        type=float,
        metavar="SECONDS",
        help="exit after this long without claiming a job (for tests and demos)",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    settings = get_settings()
    if args.concurrency:
        settings = settings.model_copy(update={"worker_concurrency": args.concurrency})
    asyncio.run(run_worker(settings, exit_when_idle=args.exit_when_idle))


if __name__ == "__main__":
    main()
