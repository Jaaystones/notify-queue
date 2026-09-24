"""The headline guarantee: no duplicate delivery with many workers polling at once.

Two variants:
  * in-process: 20 workers spread over 4 connection pools (as if 4 processes), with a
    30% random failure rate so the retry path races too;
  * multi-process: 4 real ``python -m notify_queue.worker`` OS processes.

Both assert against the mock provider's log, which counts every request it gets
per job, including duplicates it absorbed via the idempotency key. So "zero
duplicate_requests" means the queue itself never sent anything twice; the provider
key was never needed.
"""

import asyncio
import os
import sys

from sqlalchemy.ext.asyncio import AsyncEngine

from notify_queue.db.session import create_engine
from notify_queue.worker.common import CacheInvalidator
from tests.conftest import TEST_DATABASE_URL, TEST_REDIS_URL, make_settings
from tests.factories import build_worker, column, insert_jobs, scalar

NON_TERMINAL = "SELECT count(*) FROM jobs WHERE status IN ('pending', 'processing')"


async def assert_delivered_exactly_once(engine: AsyncEngine, total: int) -> None:
    assert await scalar(engine, NON_TERMINAL) == 0
    sent = await scalar(engine, "SELECT count(*) FROM jobs WHERE status = 'sent'")
    dead = await scalar(engine, "SELECT count(*) FROM jobs WHERE status = 'dead_lettered'")
    assert sent + dead == total

    # Nothing was ever sent twice, not even an absorbed duplicate. (A duplicate is
    # possible in production when a send times out after the provider accepted it;
    # the provider key absorbs it. Pools are sized below so that cannot happen here,
    # and the message shows the attempt history if it ever does.)
    duplicated = await column(
        engine,
        """
        SELECT a.job_id || ' #' || a.attempt_no || ' ' || a.outcome || ': ' || coalesce(a.error, '')
        FROM mock_provider_log p JOIN job_attempts a ON a.job_id = p.job_id
        WHERE p.duplicate_requests > 0 ORDER BY a.job_id, a.attempt_no
        """,
    )
    assert duplicated == [], duplicated
    # Every sent job reached the provider exactly once and has one ledger row.
    assert await scalar(engine, "SELECT count(*) FROM mock_provider_log") == sent
    assert await scalar(engine, "SELECT count(*) FROM deliveries") == sent
    assert (
        await scalar(
            engine,
            """
            SELECT count(*) FROM jobs j
            JOIN mock_provider_log p ON p.job_id = j.id
            JOIN deliveries d ON d.job_id = j.id
            WHERE j.status = 'sent'
            """,
        )
        == sent
    )
    # A dead-lettered job was never delivered.
    assert (
        await scalar(
            engine,
            """
            SELECT count(*) FROM mock_provider_log p
            JOIN jobs j ON j.id = p.job_id WHERE j.status = 'dead_lettered'
            """,
        )
        == 0
    )
    assert await scalar(engine, "SELECT count(*) FROM job_attempts WHERE outcome = 'sent'") == sent
    assert await scalar(engine, "SELECT count(*) FROM dead_letters") == dead


async def test_no_duplicate_delivery_with_20_concurrent_workers(engine: AsyncEngine) -> None:
    total = 1000
    await insert_jobs(engine, total, max_attempts=5)
    settings = make_settings(
        failure_rate=0.3,
        # Each in-flight job holds at most one connection at a time, so a pool of
        # 5 workers x batch 3 + claims never waits for a connection.
        batch_size=3,
        poll_interval=0.02,
        backoff_base_seconds=0.01,
        backoff_cap_seconds=0.05,
        rate_limit_per_hour=1_000_000,
        worker_concurrency=1,
        db_pool_size=20,
        db_max_overflow=0,
    )
    pools = [create_engine(settings) for _ in range(4)]
    workers = [build_worker(pools[i % 4], settings, worker_id=f"worker-{i}") for i in range(20)]
    stop = asyncio.Event()

    async def stop_when_drained() -> None:
        while await scalar(engine, NON_TERMINAL):  # noqa: ASYNC110 - polls DB state
            await asyncio.sleep(0.2)
        stop.set()

    try:
        await asyncio.wait_for(
            asyncio.gather(stop_when_drained(), *(w.run(stop) for w in workers)), timeout=180
        )
    finally:
        for pool in pools:
            await pool.dispose()

    await assert_delivered_exactly_once(engine, total)
    # Sanity: work really was spread across workers, and failures really happened.
    busy = [w for w in workers if w.stats["sent"] + w.stats["dead_lettered"] > 0]
    assert len(busy) >= 10
    assert sum(w.stats["failed_attempts"] for w in workers) > 100
    assert sum(w.stats["lease_lost"] for w in workers) == 0


async def test_no_duplicate_delivery_across_worker_processes(engine: AsyncEngine) -> None:
    total = 500
    # Hold the jobs back briefly so every process is up and polling before any job
    # is due; otherwise a fast process can drain the queue before a slow one starts.
    await insert_jobs(engine, total, max_attempts=5, delay_seconds=4)
    env = {
        **os.environ,
        "DATABASE_URL": TEST_DATABASE_URL,
        "REDIS_URL": TEST_REDIS_URL,
        "REDIS_KEY_PREFIX": "nqtest:procs:",
        "FAILURE_RATE": "0.3",
        "BATCH_SIZE": "5",
        "POLL_INTERVAL": "0.05",
        "BACKOFF_BASE_SECONDS": "0.01",
        "BACKOFF_CAP_SECONDS": "0.05",
        "RATE_LIMIT_PER_HOUR": "1000000",
        "MOCK_LATENCY_MIN": "0",
        "MOCK_LATENCY_MAX": "0.005",
        "DB_POOL_SIZE": "5",
        "DB_MAX_OVERFLOW": "5",
        # Nothing listens here; webhook POSTs fail fast and are retried later.
        "DEFAULT_CALLBACK_URL": "http://127.0.0.1:9/",
    }
    procs = [
        await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "notify_queue.worker",
            "--concurrency",
            "4",
            "--exit-when-idle",
            "8",
            env={**env, "WORKER_ID": f"proc-{i}"},
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
        )
        for i in range(4)
    ]
    results = await asyncio.wait_for(asyncio.gather(*(p.communicate() for p in procs)), timeout=180)

    for proc, (_, stderr) in zip(procs, results, strict=True):
        assert proc.returncode == 0, stderr.decode()[-2000:]
    await assert_delivered_exactly_once(engine, total)
    workers_used = await scalar(
        engine, "SELECT count(DISTINCT split_part(worker_id, '/', 1)) FROM deliveries"
    )
    assert workers_used >= 3  # the work really was shared between processes


async def test_invalidator_is_optional() -> None:
    # Workers run fine without a cache (CACHE_ENABLED=false).
    await CacheInvalidator(None).drain()
