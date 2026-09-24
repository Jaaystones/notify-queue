import uuid
from datetime import UTC, datetime

from httpx import ASGITransport, AsyncClient

from notify_queue.api.main import create_app
from notify_queue.cache.job_cache import JobCache
from notify_queue.domain.enums import Channel, JobStatus, Priority
from notify_queue.domain.schemas import JobOut
from tests.conftest import make_settings
from tests.factories import job_json


def _job(version: int, status: JobStatus = JobStatus.PENDING, job_id: uuid.UUID | None = None):
    now = datetime.now(UTC)
    return JobOut(
        id=job_id or uuid.uuid4(),
        idempotency_key=None,
        recipient="user@example.com",
        channel=Channel.EMAIL,
        payload={},
        priority=Priority.NORMAL,
        status=status,
        retrying=False,
        run_at=now,
        attempts=0,
        max_attempts=3,
        last_error=None,
        callback_url=None,
        version=version,
        created_at=now,
        updated_at=now,
        sent_at=None,
    )


async def test_put_and_get_round_trip(job_cache: JobCache) -> None:
    job = _job(version=1)
    assert await job_cache.put(job)
    assert await job_cache.get(job.id) == job


async def test_older_version_cannot_overwrite_newer(job_cache: JobCache) -> None:
    job_id = uuid.uuid4()
    await job_cache.put(_job(version=5, job_id=job_id, status=JobStatus.SENT))

    written = await job_cache.put(_job(version=4, job_id=job_id))

    assert not written
    cached = await job_cache.get(job_id)
    assert cached is not None and cached.version == 5


async def test_tombstone_blocks_stale_reader_but_not_fresh_one(job_cache: JobCache) -> None:
    job_id = uuid.uuid4()
    await job_cache.put(_job(version=3, job_id=job_id))

    # A worker commits version 4 and invalidates.
    await job_cache.invalidate(job_id, new_version=4)
    assert await job_cache.get(job_id) is None

    # A reader that loaded version 3 before the commit tries to write it back.
    assert not await job_cache.put(_job(version=3, job_id=job_id))
    # A reader that loaded version 4 after the commit is allowed in.
    assert await job_cache.put(_job(version=4, job_id=job_id))
    cached = await job_cache.get(job_id)
    assert cached is not None and cached.version == 4


async def test_status_read_is_cached(client: AsyncClient) -> None:
    job_id = (await client.post("/v1/jobs", json=job_json())).json()["id"]
    job_cache = client._transport.app.state.job_service._cache  # type: ignore[attr-defined]

    cached = await job_cache.get(uuid.UUID(job_id))

    assert cached is not None and str(cached.id) == job_id


async def test_idempotency_fast_path_is_populated(client: AsyncClient) -> None:
    first = await client.post("/v1/jobs", json=job_json(), headers={"Idempotency-Key": "k1"})
    job_cache = client._transport.app.state.job_service._cache  # type: ignore[attr-defined]

    cached = await job_cache.get_idempotency("k1")

    assert cached is not None and str(cached[0]) == first.json()["id"]


async def test_api_works_with_redis_unreachable(engine) -> None:
    """Fail-open: nothing listens on port 1, so every cache call errors out."""
    settings = make_settings(
        redis_url="redis://localhost:1/0", redis_socket_timeout=0.2, redis_connect_timeout=0.2
    )
    app = create_app(settings)
    async with app.router.lifespan_context(app):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as client:
            headers = {"Idempotency-Key": "k-down"}
            created = await client.post("/v1/jobs", json=job_json(), headers=headers)
            replay = await client.post("/v1/jobs", json=job_json(), headers=headers)
            fetched = await client.get(f"/v1/jobs/{created.json()['id']}")
            health = await client.get("/healthz")

    assert created.status_code == 201
    assert replay.status_code == 200 and replay.json()["id"] == created.json()["id"]
    assert fetched.status_code == 200
    assert health.json()["redis"] == "unavailable"


async def test_api_works_with_cache_disabled(engine) -> None:
    app = create_app(make_settings(cache_enabled=False))
    async with app.router.lifespan_context(app):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as client:
            created = await client.post("/v1/jobs", json=job_json())
            fetched = await client.get(f"/v1/jobs/{created.json()['id']}")

    assert fetched.status_code == 200
