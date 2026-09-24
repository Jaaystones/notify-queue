"""Delivers status-change events from the transactional outbox to callback URLs.

At-least-once: an event is only marked delivered after a 2xx, so a crash between
the POST and the update causes a redelivery. Receivers dedupe on ``event_id``.
"""

import asyncio
import logging

import httpx
from sqlalchemy.ext.asyncio import AsyncEngine

from notify_queue.config import Settings
from notify_queue.domain.backoff import backoff_delay
from notify_queue.repositories import webhooks as webhooks_repo
from notify_queue.repositories.webhooks import OutboxEvent
from notify_queue.worker.common import sleep_or_stop

log = logging.getLogger(__name__)

WEBHOOK_BACKOFF_BASE = 1.0
WEBHOOK_BACKOFF_CAP = 300.0


class WebhookDispatcher:
    def __init__(self, engine: AsyncEngine, http: httpx.AsyncClient, settings: Settings) -> None:
        self._engine = engine
        self._http = http
        self._settings = settings

    async def run(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            try:
                sent = await self.run_once()
            except Exception:
                log.exception("webhook dispatch pass failed")
                sent = 0
            if sent == 0:
                await sleep_or_stop(stop, self._settings.webhook_poll_interval)

    async def run_once(self) -> int:
        async with self._engine.begin() as conn:
            events = await webhooks_repo.claim_outbox_events(
                conn,
                limit=self._settings.webhook_batch_size,
                lease_seconds=self._settings.webhook_timeout_seconds * 2,
            )
        await asyncio.gather(*(self._dispatch(event) for event in events))
        return len(events)

    async def _dispatch(self, event: OutboxEvent) -> None:
        error: str | None = None
        try:
            response = await self._http.post(
                event.target_url,
                json=event.payload,
                headers={"X-Notify-Event-Id": str(event.id), "X-Notify-Event": event.event},
                timeout=self._settings.webhook_timeout_seconds,
            )
            if not response.is_success:
                error = f"HTTP {response.status_code}"
        except httpx.HTTPError as exc:
            error = f"{type(exc).__name__}: {exc}"

        async with self._engine.begin() as conn:
            if error is None:
                await webhooks_repo.mark_delivered(conn, event.id)
                return
            give_up = event.attempts >= self._settings.webhook_max_attempts
            await webhooks_repo.mark_attempt_failed(
                conn,
                event.id,
                error=error,
                retry_delay=None
                if give_up
                else backoff_delay(
                    event.attempts, base=WEBHOOK_BACKOFF_BASE, cap=WEBHOOK_BACKOFF_CAP
                ),
            )
        log.info(
            "webhook %s for job %s failed (%s)%s",
            event.event,
            event.job_id,
            error,
            "; giving up" if give_up else "",
        )
