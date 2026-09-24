from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status

from notify_queue.services.dead_letters import DeadLetterService, NotDeadLettered

router = APIRouter(prefix="/v1/dead-letters", tags=["dead letters"])


def get_dead_letter_service(request: Request) -> DeadLetterService:
    return request.app.state.dead_letter_service


Service = Annotated[DeadLetterService, Depends(get_dead_letter_service)]


@router.get("")
async def list_dead_letters(
    service: Service,
    limit: Annotated[int, Query(ge=1, le=500)] = 50,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> list[dict[str, Any]]:
    return await service.list(limit=limit, offset=offset)


@router.post("/{job_id}/requeue", status_code=status.HTTP_202_ACCEPTED)
async def requeue_dead_letter(
    job_id: UUID,
    service: Service,
    request: Request,
    extra_attempts: Annotated[int | None, Query(ge=1, le=20)] = None,
) -> dict[str, str]:
    try:
        await service.requeue(
            job_id, extra_attempts=extra_attempts or request.app.state.settings.max_attempts
        )
    except NotDeadLettered as exc:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND, "job is not in the dead letter queue"
        ) from exc
    return {"status": "requeued", "job_id": str(job_id)}
