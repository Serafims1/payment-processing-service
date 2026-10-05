import asyncio
from datetime import timedelta
from uuid import uuid4

import httpx
import pytest
from sqlalchemy import event, func, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from payments.api import create_app
from payments.exceptions import IdempotencyConflict, PaymentNotFound
from payments.models import Outbox, Payment, utcnow
from payments.repositories import OutboxRepository
from payments.schemas import PaymentCreate

pytestmark = pytest.mark.integration


async def counts(db):
    async with db.sessions() as session:
        return (
            await session.scalar(select(func.count()).select_from(Payment)),
            await session.scalar(select(func.count()).select_from(Outbox)),
        )


async def test_atomic_creation_and_concurrent_idempotency(db, repository, payment_request):
    results = await asyncio.gather(
        *(repository.create("concurrent", payment_request) for _ in range(12))
    )
    assert len({item.id for item in results}) == 1
    assert await counts(db) == (1, 1)
    other = PaymentCreate.model_validate(payment_request.model_dump() | {"amount": "13.00"})
    with pytest.raises(IdempotencyConflict):
        await repository.create("concurrent", other)
    assert await counts(db) == (1, 1)


async def test_transaction_rolls_back_both_on_outbox_failure(db, repository, payment_request):
    def fail_outbox(session, flush_context, instances):
        if any(isinstance(obj, Outbox) for obj in session.new):
            raise RuntimeError("Outbox insert failed")

    event.listen(Session, "before_flush", fail_outbox)
    try:
        with pytest.raises(RuntimeError, match="Outbox insert failed"):
            await repository.create("rollback", payment_request)
    finally:
        event.remove(Session, "before_flush", fail_outbox)
    assert await counts(db) == (0, 0)


async def test_database_unique_constraint(db, repository, payment_request):
    payment = await repository.create("unique", payment_request)
    async with db.sessions() as session, session.begin():
        existing = Payment(
            id=uuid4(),
            amount=payment.amount,
            currency=payment.currency,
            description=payment.description,
            metadata_json={},
            webhook_url=payment.webhook_url,
            idempotency_key="unique",
            request_payload={},
        )
        session.add(existing)
        with pytest.raises(IntegrityError):
            await session.flush()
        await session.rollback()
    assert await counts(db) == (1, 1)


async def test_terminal_status_and_attempt_budget(repository, payment_request):
    payment = await repository.create("terminal", payment_request)
    terminal = await repository.finish(payment.id, "succeeded")
    duplicate = await repository.finish(payment.id, "failed")
    assert duplicate.status == "succeeded"
    assert duplicate.processed_at == terminal.processed_at
    assert terminal.processed_at.utcoffset().total_seconds() == 0
    assert [await repository.begin_attempt(payment.id) for _ in range(4)] == [1, 2, 3, None]
    await repository.delivered(payment.id)
    assert (await repository.get(payment.id)).webhook_delivered_at is not None
    assert await repository.begin_attempt(payment.id) is None
    with pytest.raises(PaymentNotFound):
        await repository.get(uuid4())


async def test_outbox_claims_retry_expiry_and_stale_token(db, repository, payment_request):
    await repository.create("outbox", payment_request)
    outbox = OutboxRepository(db.sessions)
    claimed = await outbox.claim()
    assert claimed is not None
    event_id, token, _ = claimed
    assert await outbox.claim() is None
    await outbox.complete(event_id, uuid4(), published=True)
    async with db.sessions() as session:
        assert (await session.get(Outbox, event_id)).published_at is None
    await outbox.complete(event_id, token, published=False)
    claimed = await outbox.claim()
    assert claimed is not None
    async with db.sessions.begin() as session:
        await session.execute(
            update(Outbox)
            .where(Outbox.id == event_id)
            .values(leased_until=utcnow() - timedelta(seconds=1))
        )
    reclaimed = await outbox.claim()
    assert reclaimed is not None
    await outbox.complete(event_id, reclaimed[1], published=True)
    assert await outbox.claim() is None


async def test_real_api_lifecycle_health_idempotency(db, settings, payment_request):
    app = create_app(settings)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            assert (await client.get("/health")).status_code == 200
            headers = {"X-API-Key": "test-api-key", "Idempotency-Key": "api-real"}
            body = payment_request.model_dump(mode="json")
            responses = await asyncio.gather(
                *(client.post("/api/v1/payments", headers=headers, json=body) for _ in range(8))
            )
            assert all(r.status_code == 202 for r in responses)
            assert len({r.json()["payment_id"] for r in responses}) == 1
            payment_id = responses[0].json()["payment_id"]
            assert (
                await client.get(f"/api/v1/payments/{payment_id}", headers=headers)
            ).status_code == 200
            assert (
                await client.post(
                    "/api/v1/payments", headers=headers, json=body | {"currency": "USD"}
                )
            ).status_code == 409
    assert await counts(db) == (1, 1)
