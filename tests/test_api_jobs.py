import asyncio
from datetime import UTC, datetime, timedelta

from httpx import AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from tests.factories import iso, job_json


async def test_schedule_job_immediately(client: AsyncClient) -> None:
    resp = await client.post("/v1/jobs", json=job_json(priority="high"))

    assert resp.status_code == 201
    job = resp.json()
    assert job["status"] == "pending"
    assert job["priority"] == "high"
    assert job["attempts"] == 0
    assert job["max_attempts"] == 5


async def test_schedule_with_delay_sets_future_run_at(client: AsyncClient) -> None:
    resp = await client.post("/v1/jobs", json=job_json(delay_seconds=3600))

    assert resp.status_code == 201
    run_at = datetime.fromisoformat(resp.json()["run_at"])
    assert run_at > datetime.now(UTC) + timedelta(minutes=59)


async def test_schedule_with_send_at(client: AsyncClient) -> None:
    send_at = datetime.now(UTC) + timedelta(days=1)
    resp = await client.post("/v1/jobs", json=job_json(send_at=iso(send_at)))

    assert resp.status_code == 201
    assert datetime.fromisoformat(resp.json()["run_at"]) == send_at


async def test_rejects_send_at_and_delay_together(client: AsyncClient) -> None:
    send_at = datetime.now(UTC) + timedelta(hours=1)
    resp = await client.post("/v1/jobs", json=job_json(send_at=iso(send_at), delay_seconds=10))
    assert resp.status_code == 422


async def test_rejects_send_at_in_the_past(client: AsyncClient) -> None:
    send_at = datetime.now(UTC) - timedelta(hours=1)
    resp = await client.post("/v1/jobs", json=job_json(send_at=iso(send_at)))
    assert resp.status_code == 422


async def test_rejects_naive_send_at_and_bad_priority(client: AsyncClient) -> None:
    naive = (datetime.now(UTC) + timedelta(hours=1)).replace(tzinfo=None)
    assert (await client.post("/v1/jobs", json=job_json(send_at=iso(naive)))).status_code == 422
    assert (await client.post("/v1/jobs", json=job_json(priority="urgent"))).status_code == 422
    assert (await client.post("/v1/jobs", json=job_json(channel="fax"))).status_code == 422


async def test_get_job_status(client: AsyncClient) -> None:
    created = (await client.post("/v1/jobs", json=job_json())).json()

    resp = await client.get(f"/v1/jobs/{created['id']}", params={"include_attempts": True})

    assert resp.status_code == 200
    assert resp.json()["id"] == created["id"]
    assert resp.json()["attempt_history"] == []


async def test_get_unknown_job_is_404(client: AsyncClient) -> None:
    resp = await client.get("/v1/jobs/00000000-0000-0000-0000-000000000000")
    assert resp.status_code == 404


async def test_idempotent_replay_returns_same_job(client: AsyncClient) -> None:
    headers = {"Idempotency-Key": "order-123-welcome"}
    first = await client.post("/v1/jobs", json=job_json(), headers=headers)
    second = await client.post("/v1/jobs", json=job_json(), headers=headers)

    assert first.status_code == 201
    assert second.status_code == 200
    assert second.json()["id"] == first.json()["id"]


async def test_idempotency_key_in_body(client: AsyncClient) -> None:
    first = await client.post("/v1/jobs", json=job_json(idempotency_key="k-body"))
    second = await client.post("/v1/jobs", json=job_json(idempotency_key="k-body"))
    assert second.status_code == 200
    assert second.json()["id"] == first.json()["id"]


async def test_idempotency_key_reused_with_different_body_is_409(client: AsyncClient) -> None:
    headers = {"Idempotency-Key": "k-conflict"}
    await client.post("/v1/jobs", json=job_json(), headers=headers)

    resp = await client.post("/v1/jobs", json=job_json(recipient="other@x.com"), headers=headers)

    assert resp.status_code == 409


async def test_concurrent_duplicate_submissions_create_one_job(
    client: AsyncClient, engine: AsyncEngine
) -> None:
    headers = {"Idempotency-Key": "k-race"}

    responses = await asyncio.gather(
        *(client.post("/v1/jobs", json=job_json(), headers=headers) for _ in range(50))
    )

    assert sorted(r.status_code for r in responses) == [200] * 49 + [201]
    assert len({r.json()["id"] for r in responses}) == 1
    async with engine.connect() as conn:
        count = await conn.scalar(text("SELECT count(*) FROM jobs"))
    assert count == 1


async def test_healthz(client: AsyncClient) -> None:
    resp = await client.get("/healthz")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok", "postgres": "ok", "redis": "ok"}
