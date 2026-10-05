from datetime import timedelta
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import or_, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payments.exceptions import IdempotencyConflict, PaymentNotFound
from payments.models import Outbox, Payment, utcnow
from payments.schemas import PaymentCreate


class PaymentRepository:
    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self.sessions = sessions

    async def get(self, payment_id: UUID) -> Payment:
        async with self.sessions() as session:
            payment = await session.get(Payment, payment_id)
            if payment is None:
                raise PaymentNotFound("Payment not found")
            return payment

    async def create(self, key: str, request: PaymentCreate) -> Payment:
        payload = request.canonical_payload()
        payment = Payment(
            id=uuid4(),
            amount=request.amount,
            currency=request.currency,
            description=request.description,
            metadata_json=request.metadata,
            webhook_url=str(request.webhook_url),
            idempotency_key=key,
            request_payload=payload,
        )
        async with self.sessions() as session:
            try:
                async with session.begin():
                    session.add(payment)
                    await session.flush()
                    session.add(
                        Outbox(
                            payment_id=payment.id,
                            event_type="payment.created",
                            payload={"payment_id": str(payment.id)},
                        )
                    )
            except IntegrityError:
                row = (
                    await session.execute(
                        select(Payment, Payment.request_payload == payload).where(
                            Payment.idempotency_key == key
                        )
                    )
                ).one_or_none()
                if row is None:
                    raise
                existing, matches = row
                # JSONB equality keeps booleans distinct from numbers, including nested values.
                if not matches:
                    raise IdempotencyConflict("Idempotency key payload mismatch") from None
                return existing
        return payment

    async def finish(self, payment_id: UUID, status: str) -> Payment:
        async with self.sessions.begin() as session:
            await session.execute(
                update(Payment)
                .where(Payment.id == payment_id, Payment.status == "pending")
                .values(status=status, processed_at=utcnow())
            )
        return await self.get(payment_id)

    async def begin_attempt(self, payment_id: UUID) -> int | None:
        async with self.sessions.begin() as session:
            return await session.scalar(
                update(Payment)
                .where(
                    Payment.id == payment_id,
                    Payment.webhook_attempts < 3,
                    Payment.webhook_delivered_at.is_(None),
                )
                .values(webhook_attempts=Payment.webhook_attempts + 1)
                .returning(Payment.webhook_attempts)
            )

    async def delivered(self, payment_id: UUID) -> None:
        async with self.sessions.begin() as session:
            await session.execute(
                update(Payment)
                .where(Payment.id == payment_id)
                .values(webhook_delivered_at=utcnow())
            )


class OutboxRepository:
    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self.sessions = sessions

    async def claim(self) -> tuple[UUID, UUID, dict[str, Any]] | None:
        async with self.sessions.begin() as session:
            event = await session.scalar(
                select(Outbox)
                .where(
                    Outbox.published_at.is_(None),
                    or_(Outbox.leased_until.is_(None), Outbox.leased_until < utcnow()),
                )
                .order_by(Outbox.created_at)
                .with_for_update(skip_locked=True)
                .limit(1)
            )
            if event is None:
                return None
            token = uuid4()
            event.lease_token = token
            event.leased_until = utcnow() + timedelta(seconds=30)
            return event.id, token, event.payload

    async def complete(self, event_id: UUID, token: UUID, *, published: bool) -> None:
        async with self.sessions.begin() as session:
            await session.execute(
                update(Outbox)
                .where(
                    Outbox.id == event_id,
                    Outbox.lease_token == token,
                )
                .values(
                    published_at=utcnow() if published else None,
                    leased_until=None,
                    lease_token=None,
                )
            )
