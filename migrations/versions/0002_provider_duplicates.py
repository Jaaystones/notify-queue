"""count duplicate requests at the mock provider

The mock provider dedupes on the idempotency key (as SES/Twilio do), so a second
send for the same job is absorbed rather than delivered. Counting those absorbed
requests lets tests tell "our system never re-sent" apart from "the provider's
idempotency key caught a re-send".

Revision ID: 0002
Revises: 0001
Create Date: 2026-09-25
"""

from alembic import op

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        "ALTER TABLE mock_provider_log ADD COLUMN duplicate_requests integer NOT NULL DEFAULT 0"
    )


def downgrade() -> None:
    op.execute("ALTER TABLE mock_provider_log DROP COLUMN duplicate_requests")
