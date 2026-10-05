"""Run against a real Compose stack from inside the API container."""

import asyncio
import json
import logging
import sys
from uuid import UUID, uuid4

import httpx
from faststream.rabbit import Channel, RabbitBroker
from sqlalchemy import func, select

from payments.config import Settings
from payments.database import Database
from payments.messaging import DLQ, publish
from payments.models import Outbox, Payment

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def poll(predicate, deadline_seconds=40):
    async with asyncio.timeout(deadline_seconds):
        while not await predicate():  # noqa: ASYNC110 - observe external service state
            await asyncio.sleep(0.1)


async def run():
    settings = Settings()
    db = Database(settings)
    mode = sys.argv[1] if len(sys.argv) > 1 else "normal"
    attempts = {}
    received = {}

    async def handle(reader, writer):
        headers = await reader.readuntil(b"\r\n\r\n")
        size = next(
            int(line.split(b":", 1)[1])
            for line in headers.split(b"\r\n")
            if line.lower().startswith(b"content-length:")
        )
        body = json.loads(await reader.readexactly(size))
        path = headers.split(b" ")[1].decode()
        payment_id = body["payment_id"]
        attempts[payment_id] = attempts.get(payment_id, 0) + 1
        received[payment_id] = body
        code = 500 if path == "/fail" or (path == "/retry" and attempts[payment_id] < 3) else 200
        writer.write(
            f"HTTP/1.1 {code} Result\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".encode()
        )
        await writer.drain()
        writer.close()
        await writer.wait_closed()

    async with httpx.AsyncClient(
        base_url="http://localhost:8000",
        timeout=5,
        headers={"X-API-Key": settings.api_key.get_secret_value()},
    ) as client:
        try:
            if mode == "outage-create":
                body = {
                    "amount": "7.50",
                    "currency": "EUR",
                    "description": "outage-e2e",
                    "metadata": {},
                    "webhook_url": "http://api:1/unreachable",
                }
                result = await client.post(
                    "/api/v1/payments", json=body, headers={"Idempotency-Key": "outage-e2e"}
                )
                assert result.status_code == 202
                await asyncio.sleep(2)
                async with db.sessions() as session:
                    payment = await session.scalar(
                        select(Payment).where(Payment.idempotency_key == "outage-e2e")
                    )
                    outbox = await session.scalar(
                        select(Outbox).where(Outbox.payment_id == payment.id)
                    )
                    assert outbox.published_at is None
                    assert payment.status == "pending"
                logger.info("Broker outage: API accepted payment, outbox retained")
                return
            if mode == "outage-check":

                async def recovered():
                    async with db.sessions() as session:
                        payment = await session.scalar(
                            select(Payment).where(Payment.idempotency_key == "outage-e2e")
                        )
                        outbox = await session.scalar(
                            select(Outbox).where(Outbox.payment_id == payment.id)
                        )
                        return outbox.published_at is not None and payment.status != "pending"

                await poll(recovered, 90)
                logger.info("Broker recovery: retained outbox published and processed")
                return
            server = await asyncio.start_server(handle, "0.0.0.0", 0)
            port = server.sockets[0].getsockname()[1]
            broker = RabbitBroker(
                settings.rabbitmq_url.get_secret_value(),
                default_channel=Channel(on_return_raises=True),
            )
            await broker.connect()
            try:
                async with server:
                    for path, expected in [("/ok", 1), ("/retry", 3), ("/fail", 3)]:
                        key = str(uuid4())
                        body = {
                            "amount": "12.30",
                            "currency": "RUB",
                            "description": "E2E",
                            "metadata": {"path": path},
                            "webhook_url": f"http://api:{port}{path}",
                        }
                        headers = {"Idempotency-Key": key}
                        result = await client.post("/api/v1/payments", json=body, headers=headers)
                        assert result.status_code == 202 and result.json()["status"] == "pending"
                        payment_id = result.json()["payment_id"]
                        duplicate = await client.post(
                            "/api/v1/payments", json=body, headers=headers
                        )
                        assert duplicate.json()["payment_id"] == payment_id
                        conflict = await client.post(
                            "/api/v1/payments", json=body | {"amount": "13.00"}, headers=headers
                        )
                        assert conflict.status_code == 409
                        invalid = await client.get(
                            f"/api/v1/payments/{payment_id}", headers={"X-API-Key": "invalid"}
                        )
                        assert invalid.status_code == 401

                        async def completed(payment_id=payment_id, expected=expected, path=path):
                            async with db.sessions() as session:
                                item = await session.get(Payment, UUID(payment_id))
                                return item.webhook_attempts == expected and (
                                    item.webhook_delivered_at is not None
                                    if path != "/fail"
                                    else attempts.get(payment_id, 0) == expected
                                )

                        await poll(completed)
                        detail = await client.get(f"/api/v1/payments/{payment_id}")
                        assert detail.json()["status"] in ("succeeded", "failed")
                        assert received[payment_id]["amount"] == "12.30"
                        original_time = detail.json()["processed_at"]
                        async with db.sessions() as session:
                            assert (
                                await session.scalar(
                                    select(func.count())
                                    .select_from(Payment)
                                    .where(Payment.idempotency_key == key)
                                )
                                == 1
                            )
                            event = await session.scalar(
                                select(Outbox).where(Outbox.payment_id == UUID(payment_id))
                            )
                            assert event.published_at is not None
                        if path == "/fail":
                            queue = await broker.declare_queue(DLQ)

                            async def dead_lettered(queue=queue, payment_id=payment_id):
                                message = await queue.get(fail=False)
                                if message is None:
                                    return False
                                matches = json.loads(message.body).get("payment_id") == payment_id
                                await message.ack()
                                return matches

                            await poll(dead_lettered)
                        await publish(broker, {"payment_id": payment_id}, str(uuid4()))
                        # FIFO barrier confirms the single consumer finished the duplicate.
                        barrier = await client.post(
                            "/api/v1/payments",
                            json=body | {"webhook_url": f"http://api:{port}/ok"},
                            headers={"Idempotency-Key": str(uuid4())},
                        )
                        assert barrier.status_code == 202
                        barrier_id = barrier.json()["payment_id"]

                        async def barrier_done(barrier_id=barrier_id):
                            async with db.sessions() as session:
                                item = await session.get(Payment, UUID(barrier_id))
                                return item.webhook_delivered_at is not None

                        await poll(barrier_done)
                        again = await client.get(f"/api/v1/payments/{payment_id}")
                        assert again.json()["processed_at"] == original_time
                        assert attempts[payment_id] == expected
                        logger.info(
                            "E2E %s: idempotency, terminal state, %s attempts, duplicate OK",
                            path,
                            expected,
                        )
            finally:
                await broker.stop()
        finally:
            await db.close()


if __name__ == "__main__":
    asyncio.run(run())
