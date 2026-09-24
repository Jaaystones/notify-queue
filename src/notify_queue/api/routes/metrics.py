from typing import Annotated, Any

from fastapi import APIRouter, Depends, Request

from notify_queue.services.metrics import MetricsService

router = APIRouter(prefix="/v1", tags=["metrics"])


def get_metrics_service(request: Request) -> MetricsService:
    return request.app.state.metrics_service


@router.get("/metrics")
async def metrics(
    service: Annotated[MetricsService, Depends(get_metrics_service)],
) -> dict[str, Any]:
    """Job counts by state. Served from a 2-second Redis cache."""
    return await service.snapshot()
