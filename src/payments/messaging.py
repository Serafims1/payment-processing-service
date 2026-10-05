import asyncio
import logging
from typing import Any

from faststream.rabbit import RabbitBroker, RabbitExchange, RabbitQueue

from payments.repositories import OutboxRepository

logger = logging.getLogger(__name__)
EXCHANGE = RabbitExchange("payments", durable=True)
DLX = RabbitExchange("payments.dlx", durable=True)
ROUTING_KEY = "payments.new"
DLQ = RabbitQueue("payments.dlq", durable=True, routing_key="payments.dead")
QUEUE = RabbitQueue(
    ROUTING_KEY,
    durable=True,
    routing_key=ROUTING_KEY,
    arguments={
        "x-dead-letter-exchange": DLX.name,
        "x-dead-letter-routing-key": DLQ.routing_key,
        "x-single-active-consumer": True,
    },
)


async def declare_topology(broker: RabbitBroker) -> None:
    await broker.declare_exchange(EXCHANGE)
    await broker.declare_exchange(DLX)
    dead = await broker.declare_queue(DLQ)
    await dead.bind(DLX.name, routing_key=DLQ.routing_key)
    main = await broker.declare_queue(QUEUE)
    await main.bind(EXCHANGE.name, routing_key=ROUTING_KEY)


async def publish(broker: RabbitBroker, payload: dict[str, Any], event_id: str) -> None:
    await broker.publish(
        payload,
        exchange=EXCHANGE,
        routing_key=ROUTING_KEY,
        persist=True,
        mandatory=True,
        message_id=event_id,
        timeout=10,
    )


class Relay:
    def __init__(self, repository: OutboxRepository, broker: RabbitBroker, interval: float) -> None:
        self.repository = repository
        self.broker = broker
        self.interval = interval

    async def once(self) -> bool:
        claimed = await self.repository.claim()
        if claimed is None:
            return False
        event_id, token, payload = claimed
        try:
            await publish(self.broker, payload, str(event_id))
        except Exception:
            logger.exception("Outbox publication failed event=%s", event_id)
            await self.repository.complete(event_id, token, published=False)
            raise
        await self.repository.complete(event_id, token, published=True)
        return True

    async def run(self) -> None:
        while True:
            try:
                if await self.once():
                    continue
            except Exception:
                logger.exception("Relay iteration failed; retained for retry")
            await asyncio.sleep(self.interval)
