from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncEngine

from notify_queue.worker.webhook_dispatcher import WebhookDispatcher
from tests.conftest import make_settings
from tests.factories import build_worker, insert_jobs, job_json, scalar


async def test_status_change_is_delivered_to_callback(
    client: AsyncClient, engine: AsyncEngine
) -> None:
    job_id = (await client.post("/v1/jobs", json=job_json())).json()["id"]
    settings = make_settings(failure_rate=0)
    await build_worker(engine, settings).run_batch()

    assert await WebhookDispatcher(engine, client, settings).run_once() == 1

    received = (await client.get("/mock/webhooks", params={"job_id": job_id})).json()
    assert [r["event"] for r in received] == ["sent"]
    assert received[0]["payload"]["status"] == "sent"
    assert await scalar(engine, "SELECT status FROM webhook_outbox") == "delivered"


async def test_receiver_dedupes_redelivered_events(client: AsyncClient) -> None:
    event = {
        "event_id": "6f1c3b0e-0d6e-4b43-9c4e-2d8f0b8a1a11",
        "job_id": "0c6a3c56-1d8a-4a9e-8f7e-3a0b2c1d4e5f",
        "event": "sent",
    }
    first = await client.post("/mock/webhooks", json=event)
    second = await client.post("/mock/webhooks", json=event)

    assert first.json()["duplicate"] is False
    assert second.json()["duplicate"] is True


async def test_failed_callback_is_retried_then_abandoned(
    client: AsyncClient, engine: AsyncEngine
) -> None:
    await insert_jobs(engine, 1, callback_url="http://testserver/does-not-exist")
    settings = make_settings(failure_rate=0, webhook_max_attempts=2)
    await build_worker(engine, settings).run_batch()
    dispatcher = WebhookDispatcher(engine, client, settings)

    await dispatcher.run_once()

    assert await scalar(engine, "SELECT status FROM webhook_outbox") == "pending"
    assert await scalar(engine, "SELECT last_error FROM webhook_outbox") == "HTTP 404"
    assert await scalar(engine, "SELECT next_attempt_at > now() FROM webhook_outbox")
    assert await dispatcher.run_once() == 0  # not due yet

    async with engine.begin() as conn:
        from sqlalchemy import text

        await conn.execute(text("UPDATE webhook_outbox SET next_attempt_at = now()"))
    await dispatcher.run_once()

    assert await scalar(engine, "SELECT status FROM webhook_outbox") == "failed"
    assert await scalar(engine, "SELECT attempts FROM webhook_outbox") == 2
