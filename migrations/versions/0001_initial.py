"""initial schema

Revision ID: 0001
Revises:
Create Date: 2026-09-24
"""

from alembic import op

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None

UPGRADE = """
CREATE TYPE channel AS ENUM ('email', 'sms', 'push');
CREATE TYPE job_status AS ENUM ('pending', 'processing', 'sent', 'dead_lettered');

CREATE TABLE jobs (
    id               uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    idempotency_key  text UNIQUE,
    request_hash     text NOT NULL,
    recipient        text NOT NULL,
    channel          channel NOT NULL,
    payload          jsonb NOT NULL,
    priority         smallint NOT NULL DEFAULT 1 CHECK (priority BETWEEN 0 AND 3),
    status           job_status NOT NULL DEFAULT 'pending',
    run_at           timestamptz NOT NULL,
    attempts         integer NOT NULL DEFAULT 0,
    max_attempts     integer NOT NULL CHECK (max_attempts >= 1),
    last_error       text,
    -- Claim state. claim_token is the fencing token: every write after a claim
    -- must present it, so a worker whose lease expired cannot overwrite the job.
    claim_token      uuid,
    locked_by        text,
    claimed_at       timestamptz,
    lease_expires_at timestamptz,
    callback_url     text,
    -- Bumped on every update. The Redis cache refuses to store an older version.
    version          integer NOT NULL DEFAULT 1,
    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now(),
    sent_at          timestamptz
);

-- Serves the claim query: ORDER BY priority DESC, run_at over due pending jobs.
CREATE INDEX ix_jobs_due ON jobs (priority DESC, run_at) WHERE status = 'pending';
-- Serves the reaper: expired leases.
CREATE INDEX ix_jobs_lease ON jobs (lease_expires_at) WHERE status = 'processing';
CREATE INDEX ix_jobs_recipient ON jobs (recipient);

-- Exactly-once ledger. The primary key is the last line of defence against a
-- second delivery being recorded for the same job.
CREATE TABLE deliveries (
    job_id              uuid PRIMARY KEY REFERENCES jobs (id),
    worker_id           text NOT NULL,
    provider_message_id text,
    delivered_at        timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE dead_letters (
    job_id           uuid PRIMARY KEY REFERENCES jobs (id),
    reason           text NOT NULL,
    attempts         integer NOT NULL,
    last_error       text,
    dead_lettered_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE job_attempts (
    id          bigserial PRIMARY KEY,
    job_id      uuid NOT NULL REFERENCES jobs (id),
    attempt_no  integer NOT NULL,
    worker_id   text,
    outcome     text NOT NULL CHECK (outcome IN ('sent', 'failed', 'lease_expired')),
    error       text,
    started_at  timestamptz,
    finished_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (job_id, attempt_no)
);

CREATE TABLE rate_limit_buckets (
    recipient    text NOT NULL,
    window_start timestamptz NOT NULL,
    count        integer NOT NULL,
    PRIMARY KEY (recipient, window_start)
);

-- Transactional outbox: written in the same transaction as the status change,
-- delivered to callback URLs by the dispatcher.
CREATE TABLE webhook_outbox (
    id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id          uuid NOT NULL REFERENCES jobs (id),
    event           text NOT NULL CHECK (event IN ('sent', 'failed', 'dead_lettered')),
    payload         jsonb NOT NULL,
    target_url      text NOT NULL,
    status          text NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending', 'delivered', 'failed')),
    attempts        integer NOT NULL DEFAULT 0,
    next_attempt_at timestamptz NOT NULL DEFAULT now(),
    last_error      text,
    created_at      timestamptz NOT NULL DEFAULT now(),
    delivered_at    timestamptz
);
CREATE INDEX ix_webhook_outbox_due ON webhook_outbox (next_attempt_at) WHERE status = 'pending';

-- Stand-in for an external provider's own dedupe store (SES/Twilio idempotency keys).
-- Deliberately no FK to jobs: it models a separate system.
CREATE TABLE mock_provider_log (
    id                       bigserial PRIMARY KEY,
    provider_idempotency_key text NOT NULL UNIQUE,
    job_id                   uuid NOT NULL,
    channel                  channel NOT NULL,
    recipient                text NOT NULL,
    received_at              timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE mock_webhook_receipts (
    id          bigserial PRIMARY KEY,
    event_id    uuid NOT NULL UNIQUE,
    job_id      uuid NOT NULL,
    event       text NOT NULL,
    payload     jsonb NOT NULL,
    received_at timestamptz NOT NULL DEFAULT now()
);
"""

DOWNGRADE = """
DROP TABLE IF EXISTS mock_webhook_receipts, mock_provider_log, webhook_outbox,
    rate_limit_buckets, job_attempts, dead_letters, deliveries, jobs;
DROP TYPE IF EXISTS job_status;
DROP TYPE IF EXISTS channel;
"""


def _execute_script(script: str) -> None:
    # asyncpg prepares each statement, and a prepared statement may only hold one
    # command, so run the script one statement at a time.
    for statement in script.split(";"):
        code = "\n".join(
            line for line in statement.splitlines() if not line.strip().startswith("--")
        ).strip()
        if code:
            op.execute(code)


def upgrade() -> None:
    _execute_script(UPGRADE)


def downgrade() -> None:
    _execute_script(DOWNGRADE)
