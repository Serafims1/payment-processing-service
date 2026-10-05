import asyncio
import json
import os
from unittest.mock import AsyncMock

import pytest
from faststream.rabbit import Channel, RabbitBroker
from sqlalchemy import select

from payments.consumer import worker
from payments.messaging import DLQ, QUEUE, Relay, declare_topology, publish
from payments.models import Outbox
from payments.processing import Gateway
from payments.repositories import OutboxRepository
from payments.schemas import PaymentCreate

pytestmark = pytest.mark.integration


@pytest.fixture
async def broker(settings):
    if not os.getenv("TEST_RABBITMQ_URL"):
        pytest.skip("Set TEST_RABBITMQ_URL to a dedicated test RabbitMQ vhost")
    broker = RabbitBroker(
        settings.rabbitmq_url.get_secret_value(),
        default_channel=Channel(publisher_confirms=True, on_return_raises=True),
    )
    await broker.connect()
    await declare_topology(broker)
    for entity in (QUEUE, DLQ):
        queue = await broker.declare_queue(entity)
        await queue.purge()
    try:
        yield broker
    finally:
        await broker.stop()


@pytest.fixture
async def webhook_server():
    state = {"codes": [], "requests": []}

    async def handle(reader, writer):
        headers = await reader.readuntil(b"\r\n\r\n")
        content_length = next(
            int(line.split(b":", 1)[1])
            for line in headers.split(b"\r\n")
            if line.lower().startswith(b"content-length:")
        )
        body = await reader.readexactly(content_length)
        state["requests"].append(json.loads(body))
        codes = state["codes"]
        code = codes.pop(0) if codes else 200
        writer.write(
            f"HTTP/1.1 {code} Result\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".encode()
        )
        await writer.drain()
        writer.close()
        await writer.wait_closed()

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    state["url"] = f"http://127.0.0.1:{server.sockets[0].getsockname()[1]}/hook"
    async with server:
        yield state


async def wait_for(predicate):
    async with asyncio.timeout(10):
        while not await predicate():  # noqa: ASYNC110 - poll state in external DB/broker
            await asyncio.sleep(0.02)


@pytest.mark.parametrize("failures", [0, 1, 2, 3])
async def test_pipeline_retry_dlq_duplicate(
    db, repository, payment_request, settings, broker, webhook_server, failures
):
    webhook_server["codes"] = [500] * failures
    request = PaymentCreate.model_validate(
        payment_request.model_dump() | {"webhook_url": webhook_server["url"]}
    )
    payment = await repository.create(f"rabbit-{failures}", request)
    gateway = AsyncMock(spec=Gateway)
    gateway.process.return_value = "succeeded"
    settings.relay_interval = 0.02
    async with worker(settings, gateway) as running:

        async def finished():
            item = await repository.get(payment.id)
            if failures == 3:
                queue = await broker.declare_queue(DLQ)
                result = await queue.get(fail=False)
                if result:
                    assert json.loads(result.body)["payment_id"] == str(payment.id)
                    await result.ack()
                    return True
                return False
            return item.webhook_delivered_at is not None

        await wait_for(finished)
        item = await repository.get(payment.id)
        assert item.status == "succeeded"
        assert item.webhook_attempts == min(failures + 1, 3)
        assert len(webhook_server["requests"]) == min(failures + 1, 3)
        assert webhook_server["requests"][0]["amount"] == "12.30"
        gateway.process.assert_awaited_once()
        processed_at = item.processed_at
        await publish(running, {"payment_id": str(payment.id)}, "duplicate")
        # Barrier: a second payment is processed after the duplicate on the same single consumer.
        barrier = await repository.create("barrier", request)

        async def barrier_done():
            return (await repository.get(barrier.id)).webhook_delivered_at is not None

        await wait_for(barrier_done)
        assert gateway.process.await_count == 2
        assert (await repository.get(payment.id)).processed_at == processed_at
        assert len(webhook_server["requests"]) == min(failures + 1, 3) + 1
    async with db.sessions() as session:
        event = await session.scalar(select(Outbox).where(Outbox.payment_id == payment.id))
        assert event.published_at is not None


async def test_relay_failure_retains_event_and_recovers(db, repository, payment_request, broker):
    await repository.create("relay-recovery", payment_request)
    outbox = OutboxRepository(db.sessions)
    broken = AsyncMock(spec=RabbitBroker)
    broken.publish.side_effect = ConnectionError("broker unavailable")
    with pytest.raises(ConnectionError):
        await Relay(outbox, broken, 0).once()
    async with db.sessions() as session:
        event = await session.scalar(select(Outbox))
        assert event.published_at is None
        assert event.leased_until is None
    relay = Relay(outbox, broker, 0)
    assert await relay.once()
    assert not await relay.once()
    queue = await broker.declare_queue(QUEUE)
    message = await queue.get(timeout=2)
    assert json.loads(message.body)["payment_id"] == str(event.payment_id)
    await message.ack()


async def test_parallel_relays_do_not_claim_same_event(db, repository, payment_request, broker):
    await repository.create("parallel-relay", payment_request)
    outbox = OutboxRepository(db.sessions)
    results = await asyncio.gather(Relay(outbox, broker, 0).once(), Relay(outbox, broker, 0).once())
    assert sorted(results) == [False, True]
    queue = await broker.declare_queue(QUEUE)
    message = await queue.get(timeout=2)
    await message.ack()
    assert await queue.get(fail=False) is None


async def test_crash_after_publish_before_mark_allows_redelivery(
    db, repository, payment_request, broker, monkeypatch
):
    from datetime import timedelta

    from sqlalchemy import update

    from payments.models import utcnow

    await repository.create("crash-window", payment_request)
    outbox = OutboxRepository(db.sessions)
    real_complete = outbox.complete
    monkeypatch.setattr(outbox, "complete", AsyncMock(side_effect=RuntimeError("crash")))
    with pytest.raises(RuntimeError, match="crash"):
        await Relay(outbox, broker, 0).once()
    async with db.sessions.begin() as session:
        event = await session.scalar(select(Outbox))
        assert event.published_at is None
        assert event.leased_until is not None
        await session.execute(update(Outbox).values(leased_until=utcnow() - timedelta(seconds=1)))
    monkeypatch.setattr(outbox, "complete", real_complete)
    assert await Relay(outbox, broker, 0).once()
    queue = await broker.declare_queue(QUEUE)
    messages = [await queue.get(timeout=2), await queue.get(timeout=2)]
    assert messages[0].body == messages[1].body
    for message in messages:
        await message.ack()


async def test_cancelled_webhook_requeues_terminal_payment_without_gateway_replay(
    db, repository, payment_request, settings, broker, webhook_server, monkeypatch
):
    from payments.processing import WebhookClient

    request = PaymentCreate.model_validate(
        payment_request.model_dump() | {"webhook_url": webhook_server["url"]}
    )
    payment = await repository.create("cancelled-webhook", request)
    gateway = AsyncMock(spec=Gateway)
    gateway.process.return_value = "failed"
    interrupted = asyncio.Event()
    original_send = WebhookClient.send

    async def interrupt_first(client, item):
        if not interrupted.is_set():
            interrupted.set()
            raise asyncio.CancelledError
        await original_send(client, item)

    monkeypatch.setattr(WebhookClient, "send", interrupt_first)
    settings.relay_interval = 0.02
    async with worker(settings, gateway):
        async with asyncio.timeout(5):
            await interrupted.wait()
        saved = await repository.get(payment.id)
        assert saved.status == "failed"
        assert saved.processed_at is not None
        assert saved.webhook_attempts == 1
        assert saved.webhook_delivered_at is None
    queue = await broker.declare_queue(QUEUE)
    redelivery = await queue.get(timeout=2)
    assert redelivery.redelivered
    assert json.loads(redelivery.body)["payment_id"] == str(payment.id)
    await redelivery.reject(requeue=True)
    async with worker(settings, gateway):

        async def delivered():
            return (await repository.get(payment.id)).webhook_delivered_at is not None

        await wait_for(delivered)
    final = await repository.get(payment.id)
    assert final.status == saved.status
    assert final.processed_at == saved.processed_at
    assert final.webhook_attempts == 2
    assert len(webhook_server["requests"]) == 1
    gateway.process.assert_awaited_once()
    assert db.engine.pool.checkedout() == 0
