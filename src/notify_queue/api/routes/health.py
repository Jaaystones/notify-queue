from fastapi import APIRouter, Request
from sqlalchemy import text

router = APIRouter(tags=["health"])


@router.get("/healthz")
async def healthz(request: Request) -> dict[str, str]:
    """Postgres is required; Redis is optional (the cache fails open)."""
    async with request.app.state.engine.connect() as conn:
        await conn.execute(text("SELECT 1"))
    cache = request.app.state.cache
    if not cache.enabled:
        redis_state = "disabled"
    else:
        redis_state = "ok" if await cache.ping() else "unavailable"
    return {"status": "ok", "postgres": "ok", "redis": redis_state}
