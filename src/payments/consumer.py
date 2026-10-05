import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress
from uuid import UUID

import httpx
from faststream import AckPolicy
from faststream.rabbit import Channel, RabbitBroker

from payments.config import Settings
from payments.database import Database
from payments.messaging import EXCHANGE, QUEUE, Relay, declare_topology
from payments.processing import Gateway, Processor, WebhookClient
from payments.repositories import OutboxRepository, PaymentRepository


@asynccontextmanager
async def worker(settings: Settings, gateway: Gateway | None = None) -> AsyncIterator[RabbitBroker]:
    db = Database(settings)
    # Prefetch=1 and one active consumer prevent concurrent gateway execution.
    broker = RabbitBroker(
        settings.rabbitmq_url.get_secret_value(),
        default_channel=Channel(prefetch_count=1, publisher_confirms=True, on_return_raises=True),
    )
    task: asyncio.Task[None] | None = None
    try:
        async with httpx.AsyncClient(
            timeout=settings.webhook_timeout, follow_redirects=False
        ) as client:
            processor = Processor(
                PaymentRepository(db.sessions),
                gateway or Gateway(),
                WebhookClient(client),
                settings.retry_base,
            )

            @broker.subscriber(QUEUE, EXCHANGE, ack_policy=AckPolicy.REJECT_ON_ERROR)
            async def consume(body: dict[str, str]) -> None:
                await processor.process(UUID(body["payment_id"]))

            await broker.connect()
            await declare_topology(broker)
            await broker.start()
            task = asyncio.create_task(
                Relay(OutboxRepository(db.sessions), broker, settings.relay_interval).run()
            )
            try:
                yield broker
            finally:
                task.cancel()
                with suppress(asyncio.CancelledError):
                    await task
                await broker.stop()
    finally:
        await db.close()


async def run() -> None:
    async with worker(Settings()):
        await asyncio.Event().wait()


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
