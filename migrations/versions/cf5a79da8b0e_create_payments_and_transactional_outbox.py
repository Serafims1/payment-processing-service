"""create payments and transactional outbox"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "cf5a79da8b0e"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "payments",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("amount", sa.Numeric(precision=20, scale=2), nullable=False),
        sa.Column("currency", sa.String(length=3), nullable=False),
        sa.Column("description", sa.String(length=1000), nullable=False),
        sa.Column("metadata", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("status", sa.String(length=10), nullable=False),
        sa.Column("idempotency_key", sa.String(length=200), nullable=False),
        sa.Column("request_payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("webhook_url", sa.String(length=2048), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("webhook_attempts", sa.Integer(), nullable=False),
        sa.Column("webhook_delivered_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint("currency IN ('RUB','USD','EUR')", name="valid_currency"),
        sa.CheckConstraint("status IN ('pending','succeeded','failed')", name="valid_status"),
        sa.CheckConstraint("amount > 0", name="positive_amount"),
        sa.CheckConstraint("webhook_attempts BETWEEN 0 AND 3", name="valid_attempts"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("idempotency_key"),
    )
    op.create_table(
        "outbox",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("payment_id", sa.Uuid(), nullable=False),
        sa.Column("event_type", sa.String(length=50), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("leased_until", sa.DateTime(timezone=True), nullable=True),
        sa.Column("lease_token", sa.Uuid(), nullable=True),
        sa.ForeignKeyConstraint(
            ["payment_id"],
            ["payments.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_outbox_published_at"), "outbox", ["published_at"], unique=False)


def downgrade() -> None:
    op.drop_index(op.f("ix_outbox_published_at"), table_name="outbox")
    op.drop_table("outbox")
    op.drop_table("payments")
