from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import CheckConstraint, DateTime, ForeignKey, Numeric, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


def utcnow() -> datetime:
    return datetime.now(UTC)


class Base(DeclarativeBase):
    pass


class Payment(Base):
    __tablename__ = "payments"
    __table_args__ = (
        CheckConstraint("amount > 0", name="positive_amount"),
        CheckConstraint("status IN ('pending','succeeded','failed')", name="valid_status"),
        CheckConstraint("currency IN ('RUB','USD','EUR')", name="valid_currency"),
        CheckConstraint("webhook_attempts BETWEEN 0 AND 3", name="valid_attempts"),
    )
    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    amount: Mapped[Decimal] = mapped_column(Numeric(20, 2))
    currency: Mapped[str] = mapped_column(String(3))
    description: Mapped[str] = mapped_column(String(1000))
    metadata_json: Mapped[dict[str, Any]] = mapped_column("metadata", JSONB)
    status: Mapped[str] = mapped_column(String(10), default="pending")
    idempotency_key: Mapped[str] = mapped_column(String(200), unique=True)
    request_payload: Mapped[dict[str, Any]] = mapped_column(JSONB)
    webhook_url: Mapped[str] = mapped_column(String(2048))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    webhook_attempts: Mapped[int] = mapped_column(default=0)
    webhook_delivered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class Outbox(Base):
    __tablename__ = "outbox"
    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    payment_id: Mapped[UUID] = mapped_column(ForeignKey("payments.id"))
    event_type: Mapped[str] = mapped_column(String(50))
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True)
    leased_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    lease_token: Mapped[UUID | None]
