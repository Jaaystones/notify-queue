# Notify Queue

A distributed delayed job and notification delivery service. Clients schedule
email, SMS and push notifications for a future time or after a delay. Any number
of worker processes deliver them in priority order, with no duplicate sends,
per-recipient rate limits, retries with exponential backoff, a dead letter
queue, and webhook callbacks on every status change.

- **Stack:** Python 3.12, FastAPI, PostgreSQL 16 (the source of truth and the
  queue), Redis/Upstash (per-recipient rate limiter and read cache, with a
  Postgres fallback), SQLAlchemy 2 async + asyncpg, Alembic, pytest.
- **Design:** [DESIGN.md](DESIGN.md) covers the architecture, the exactly-once
  argument, and scaling.

## Quick start (Docker only)

```bash
cp .env.example .env              # optional: put an Upstash URL in REDIS_URL
docker compose up --build --scale worker=3
```

This starts Postgres, runs the migrations, then starts the API on
<http://localhost:8000> and three worker containers. Without an Upstash URL, the
cache uses the local Redis container. Seed a demo workload from another terminal:

```bash
docker compose exec api python seed.py
curl -s localhost:8000/v1/metrics
```

Interactive API docs are at <http://localhost:8000/docs>.

## Local development

Requirements: [uv](https://docs.astral.sh/uv/) and Docker (for Postgres and
Redis).

```bash
cp .env.example .env        # set REDIS_URL (Upstash rediss://... or local redis://localhost:6390/0)
make install                # uv sync
make up                     # Postgres on :5440, Redis on :6390
make migrate                # alembic upgrade head
make api                    # API on http://localhost:8000
make worker                 # a worker, in another terminal
make seed                   # demo workload, in another terminal
```

### Running multiple workers

Workers coordinate only through Postgres, so you can start as many as you like
and in any combination:

```bash
# Several processes, in separate terminals
WORKER_ID=w1 uv run python -m notify_queue.worker
WORKER_ID=w2 uv run python -m notify_queue.worker
WORKER_ID=w3 uv run python -m notify_queue.worker --concurrency 8

# Or containers
docker compose up --scale worker=5
```

Each process runs `WORKER_CONCURRENCY` claim loops (default 4), a reaper and a
webhook dispatcher. `Ctrl-C` or SIGTERM stops a worker gracefully: it stops
claiming, finishes its in-flight jobs, then exits.
`--exit-when-idle SECONDS` makes a worker exit after a quiet period, which is
useful for demos and scripts.

## Using Upstash for the cache

1. In the Upstash console, open your database, then **Connect**, and copy the
   `rediss://default:<password>@<endpoint>.upstash.io:6379` URL.
2. Put it in `.env` as `REDIS_URL=`. Use `rediss://` (TLS), not `redis://`.
3. `GET /healthz` should report `"redis": "ok"`.

`.env` is gitignored. Keep credentials out of `.env.example`. The test suite
always uses the local Redis container, so it never uses your Upstash quota. If
Redis is unreachable, the service keeps working: reads come from Postgres and
rate limiting switches to a Postgres limiter (see DESIGN.md, sections 5 and 6).

## API

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/v1/jobs` | Schedule a job. Returns 201 for a new job, 200 for a replay of the same idempotency key, 409 if the key was used with a different body |
| `GET` | `/v1/jobs/{id}` | Job status. Add `?include_attempts=true` for the attempt history |
| `GET` | `/v1/metrics` | Counts of pending, scheduled, failed (retrying), processing, sent and dead-lettered jobs |
| `GET` | `/v1/dead-letters` | List the dead letter queue |
| `POST` | `/v1/dead-letters/{id}/requeue` | Put a dead-lettered job back in the queue with `?extra_attempts=N` more tries |
| `POST`, `GET` | `/mock/webhooks` | Mock callback receiver and a viewer for what it received (`?job_id=` filter) |
| `GET` | `/healthz` | Postgres and Redis status |

Schedule a job:

```bash
curl -s -X POST localhost:8000/v1/jobs \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: order-1042-receipt' \
  -d '{
        "recipient": "ada@example.com",
        "channel": "email",
        "payload": {"subject": "Your receipt", "body": "Thanks!"},
        "priority": "high",
        "delay_seconds": 30,
        "callback_url": "http://localhost:8000/mock/webhooks"
      }'
```

- `channel` is `email`, `sms` or `push`.
- `priority` is `low`, `normal` (the default), `high` or `critical`.
- Give either `send_at` (ISO 8601 with a timezone) or `delay_seconds`. With
  neither, the job is sent as soon as possible.
- `max_attempts` (1–20) overrides the default retry cap.
- `callback_url` defaults to the mock receiver.
- The idempotency key can go in the `Idempotency-Key` header or the
  `idempotency_key` body field.

Payload switches for demos: `{"simulate": "permanent_failure"}` is rejected as
undeliverable and goes straight to the dead letter queue.
`{"simulate": "poison"}` raises an unexpected error on every attempt, so it is
retried and then dead-lettered.

Webhook events have this shape:

```json
{"event_id": "…", "job_id": "…", "event": "sent | failed | dead_lettered",
 "status": "…", "recipient": "…", "channel": "…", "attempts": 1, "…": "…"}
```

## Configuration

Every setting can be given as an environment variable or in `.env`.

| Variable | Default | Meaning |
|---|---|---|
| `DATABASE_URL` | `postgresql+asyncpg://notify:notify@localhost:5440/notify_queue` | Postgres |
| `REDIS_URL` | `redis://localhost:6390/0` | Redis or Upstash (`rediss://`) |
| `CACHE_ENABLED` | `true` | Turn the cache off entirely |
| `FAILURE_RATE` | `0.1` | Mock sender's random failure probability |
| `MAX_ATTEMPTS` | `5` | Default retry cap before the dead letter queue |
| `BACKOFF_BASE_SECONDS` / `BACKOFF_CAP_SECONDS` | `2` / `300` | Exponential backoff |
| `RATE_LIMIT_PER_HOUR` | `10` | Sends per recipient per window |
| `RATE_LIMIT_WINDOW_SECONDS` | `3600` | Window length (shorten it for demos) |
| `RATE_LIMIT_BACKEND` | `redis` | `redis` (sliding window, Postgres fallback) or `postgres` (fixed window only) |
| `WORKER_CONCURRENCY` | `4` | Claim loops per worker process |
| `BATCH_SIZE` | `10` | Jobs claimed per loop iteration |
| `LEASE_SECONDS` | `30` | How long a claim lasts before the reaper can take the job back |
| `SEND_TIMEOUT_SECONDS` | `10` | Upper bound on one send (must stay below the lease) |
| `WEBHOOK_MAX_ATTEMPTS` | `8` | Callback delivery attempts before giving up |
| `MOCK_WEBHOOK_FAILURE_RATE` | `0` | Make the mock receiver return 503s, to show webhook retries |

See [config.py](src/notify_queue/config.py) for the rest.

## Tests

```bash
make up      # the tests need the Postgres and Redis containers
make test    # 62 tests, about 90 seconds
```

The tests run against real Postgres and Redis, because the behaviour under test
(`SKIP LOCKED`, the Lua scripts) can't be faked faithfully. They use a
separate `notify_queue_test` database. The headline tests are:

- `tests/test_concurrency.py` checks that nothing is delivered twice. It runs
  1,000 jobs through 20 concurrent workers at a 30% failure rate, and 500 jobs
  through 4 separate worker processes. It then asserts that the mock provider
  never received a second request for any job, that every sent job has exactly
  one delivery record, and that no dead-lettered job was ever delivered.
- `tests/test_worker.py` covers backoff timing and dead-lettering, permanent
  errors, poison messages, rate-limit deferral, priority order, the reaper,
  fencing a stale worker, and the crash-after-send case.
- `tests/test_rate_limiter.py` covers the Redis sliding window: the limit,
  sliding, a reclaimed job reusing its slot, refunds, concurrent acquires, and
  the fallback to Postgres when Redis is down.
- `tests/test_claim.py` checks that concurrent claimers never claim the same job,
  and that a stale claim token is rejected.
- `tests/test_api_jobs.py` checks scheduling validation, and that 50 concurrent
  identical submissions create exactly one job.
- `tests/test_cache.py` checks the versioned cache writes, and that the API still
  works with Redis down or turned off.

## Project layout

```
src/notify_queue/
  api/            FastAPI app and routes
  services/       scheduling, rate limiting, metrics and dead letter logic
  repositories/   all SQL. jobs.py holds the concurrency-critical statements
  worker/         claim loop, reaper, webhook dispatcher, process entry point
  senders/        Sender protocol and the mock provider
  cache/          fail-open Redis client, job/idempotency cache, metrics cache
  domain/         enums, models, API schemas, backoff
migrations/       Alembic (plain SQL)
tests/            pytest suite
seed.py           demo workload, sent through the API
```
