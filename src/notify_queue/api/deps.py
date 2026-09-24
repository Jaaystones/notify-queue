from fastapi import Request

from notify_queue.services.scheduler import JobService


def get_job_service(request: Request) -> JobService:
    return request.app.state.job_service
