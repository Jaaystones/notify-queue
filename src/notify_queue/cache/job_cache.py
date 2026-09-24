"""Job status and idempotency-key caching.

Job entries are stored as ``"<version>|<json>"``. Writes go through a Lua script
that refuses to replace a newer version with an older one. That closes the classic
cache-aside race:

    reader loads job v3 from Postgres
    worker commits v4 and invalidates the key
    reader writes v3 into the cache        <- would serve stale data until TTL

Invalidation therefore does not DEL the key; it writes a *tombstone* carrying the
new version (``"4|"``). The late v3 write is rejected because 3 < 4, and the next
reader, which loads v4, is allowed to replace the tombstone (4 >= 4).
"""

from uuid import UUID

from notify_queue.cache.client import Cache
from notify_queue.config import Settings
from notify_queue.domain.schemas import JobOut

# KEYS[1] job key; ARGV[1] version; ARGV[2] json body ("" = tombstone); ARGV[3] ttl.
_SET_IF_NEWER = """
local current = redis.call('GET', KEYS[1])
if current then
  local sep = string.find(current, '|', 1, true)
  if sep then
    local cached_version = tonumber(string.sub(current, 1, sep - 1))
    if cached_version and cached_version > tonumber(ARGV[1]) then
      return 0
    end
  end
end
redis.call('SET', KEYS[1], ARGV[1] .. '|' .. ARGV[2], 'EX', tonumber(ARGV[3]))
return 1
"""


class JobCache:
    def __init__(self, cache: Cache, settings: Settings) -> None:
        self._cache = cache
        self._settings = settings
        self._set_if_newer = cache.redis.register_script(_SET_IF_NEWER) if cache.redis else None

    def _job_key(self, job_id: UUID) -> str:
        return self._cache.key("job", job_id)

    def _idem_key(self, idempotency_key: str) -> str:
        return self._cache.key("idem", idempotency_key)

    async def get(self, job_id: UUID) -> JobOut | None:
        raw = await self._cache.call("get job", lambda r: r.get(self._job_key(job_id)), None)
        if not raw:
            return None
        _, _, body = raw.partition("|")
        if not body:  # tombstone
            return None
        return JobOut.model_validate_json(body)

    async def put(self, job: JobOut) -> bool:
        """Cache a job read from Postgres, unless a newer version is already cached."""
        ttl = (
            self._settings.cache_job_ttl_terminal
            if job.status.is_terminal
            else self._settings.cache_job_ttl_active
        )
        body = job.model_dump_json(exclude={"attempt_history"})
        return await self._write(job.id, job.version, body, ttl)

    async def invalidate(self, job_id: UUID, new_version: int) -> None:
        """Call after a status change commits. Leaves a tombstone at the new version."""
        await self._write(job_id, new_version, "", self._settings.cache_tombstone_ttl)

    async def _write(self, job_id: UUID, version: int, body: str, ttl: int) -> bool:
        if self._set_if_newer is None:
            return False
        script = self._set_if_newer
        written = await self._cache.call(
            "set job",
            lambda r: script(keys=[self._job_key(job_id)], args=[version, body, ttl], client=r),
            0,
        )
        return bool(written)

    async def get_idempotency(self, idempotency_key: str) -> tuple[UUID, str] | None:
        """Return ``(job_id, request_hash)`` for a key seen before, if cached."""
        raw = await self._cache.call(
            "get idempotency", lambda r: r.get(self._idem_key(idempotency_key)), None
        )
        if not raw:
            return None
        job_id, _, request_hash = raw.partition("|")
        return UUID(job_id), request_hash

    async def put_idempotency(self, idempotency_key: str, job_id: UUID, request_hash: str) -> None:
        await self._cache.call(
            "set idempotency",
            lambda r: r.set(
                self._idem_key(idempotency_key),
                f"{job_id}|{request_hash}",
                ex=self._settings.cache_idempotency_ttl,
            ),
            None,
        )
