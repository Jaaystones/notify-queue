from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Header, HTTPException, Response, status

from notify_queue.api.deps import get_job_service
from notify_queue.domain.schemas import ErrorOut, JobOut, ScheduleJobRequest
from notify_queue.services.scheduler import IdempotencyConflict, InvalidSchedule, JobService

router = APIRouter(prefix="/v1/jobs", tags=["jobs"])


@router.post(
    "",
    response_model=JobOut,
    status_code=status.HTTP_201_CREATED,
    responses={
        200: {"model": JobOut, "description": "Replay of an earlier request with the same key"},
        409: {"model": ErrorOut, "description": "Idempotency key reused with a different body"},
    },
)
async def schedule_job(
    body: ScheduleJobRequest,
    response: Response,
    service: Annotated[JobService, Depends(get_job_service)],
    idempotency_key: Annotated[str | None, Header(max_length=255)] = None,
) -> JobOut:
    """Schedule a notification. Pass the idempotency key in the ``Idempotency-Key``
    header or the ``idempotency_key`` body field."""
    if idempotency_key is not None:
        if body.idempotency_key not in (None, idempotency_key):
            raise HTTPException(
                status.HTTP_422_UNPROCESSABLE_CONTENT,
                "Idempotency-Key header and body idempotency_key differ",
            )
        body.idempotency_key = idempotency_key

    try:
        job, created = await service.schedule(body)
    except InvalidSchedule as exc:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, str(exc)) from exc
    except IdempotencyConflict as exc:
        raise HTTPException(status.HTTP_409_CONFLICT, str(exc)) from exc

    if not created:
        response.status_code = status.HTTP_200_OK
    return job


@router.get("/{job_id}", response_model=JobOut, responses={404: {"model": ErrorOut}})
async def get_job(
    job_id: UUID,
    service: Annotated[JobService, Depends(get_job_service)],
    include_attempts: bool = False,
) -> JobOut:
    job = await service.get(job_id, include_attempts=include_attempts)
    if job is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "job not found")
    return job
