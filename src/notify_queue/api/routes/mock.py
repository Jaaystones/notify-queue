"""Mocked webhook receiver: stands in for the client system that registered a
callback URL. Stores what it receives so the demo (and tests) can inspect it."""

import random
from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, HTTPException, Query, Request, status

from notify_queue.repositories import webhooks as webhooks_repo

router = APIRouter(prefix="/mock", tags=["mock"])


@router.post("/webhooks")
async def receive_webhook(payload: dict[str, Any], request: Request) -> dict[str, Any]:
    if random.random() < request.app.state.settings.mock_webhook_failure_rate:
        # Lets the demo show the dispatcher's retries.
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "simulated receiver outage")
    if not {"event_id", "job_id", "event"} <= payload.keys():
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, "not a Notify Queue event")
    async with request.app.state.engine.begin() as conn:
        first_time = await webhooks_repo.record_receipt(conn, payload)
    return {"received": True, "duplicate": not first_time}


@router.get("/webhooks")
async def list_webhooks(
    request: Request,
    job_id: UUID | None = None,
    limit: Annotated[int, Query(ge=1, le=500)] = 100,
) -> list[dict[str, Any]]:
    async with request.app.state.engine.connect() as conn:
        return await webhooks_repo.list_receipts(conn, job_id=job_id, limit=limit)
