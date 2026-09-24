from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncEngine

from tests.conftest import make_settings
from tests.factories import build_worker, fetch_job, job_json, scalar


async def test_metrics_reports_counts_by_state(client: AsyncClient, engine: AsyncEngine) -> None:
    await client.post("/v1/jobs", json=job_json(recipient="ok"))
    await client.post("/v1/jobs", json=job_json(payload={"simulate": "permanent_failure"}))
    await client.post("/v1/jobs", json=job_json(payload={"simulate": "poison"}, max_attempts=3))
    await client.post("/v1/jobs", json=job_json(delay_seconds=3600))
    await build_worker(engine, make_settings(failure_rate=0, batch_size=10)).run_batch()

    first = (await client.get("/v1/metrics")).json()
    second = (await client.get("/v1/metrics")).json()

    assert first["sent"] == 1
    assert first["dead_lettered"] == 1
    assert first["failed"] == 1  # the poison job, waiting to retry
    assert first["scheduled"] == 1  # the delayed job
    assert first["pending"] == 2  # delayed + retrying
    assert first["processing"] == 0
    assert first["failed_attempts_total"] == 2
    assert first["cached"] is False
    assert second["cached"] is True


async def test_list_and_requeue_dead_letter(client: AsyncClient, engine: AsyncEngine) -> None:
    body = job_json(payload={"simulate": "permanent_failure"})
    job_id = (await client.post("/v1/jobs", json=body)).json()["id"]
    await build_worker(engine, make_settings(failure_rate=0)).run_batch()

    listed = (await client.get("/v1/dead-letters")).json()
    assert [d["job_id"] for d in listed] == [job_id]
    assert listed[0]["reason"] == "permanent error"

    resp = await client.post(f"/v1/dead-letters/{job_id}/requeue", params={"extra_attempts": 2})

    assert resp.status_code == 202
    job = await fetch_job(engine, listed[0]["job_id"])
    assert job.status == "pending" and job.attempts == 1 and job.max_attempts == 3
    assert await scalar(engine, "SELECT count(*) FROM dead_letters") == 0
    # Status reads see the requeue, not a stale cached 'dead_lettered'.
    assert (await client.get(f"/v1/jobs/{job_id}")).json()["status"] == "pending"
    again = await client.post(f"/v1/dead-letters/{job_id}/requeue")
    assert again.status_code == 404
