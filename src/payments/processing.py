import asyncio
import logging
import random
from collections.abc import Awaitable, Callable
from uuid import UUID

import httpx

from payments.exceptions import DeliveryExhausted
from payments.models import Payment
from payments.repositories import PaymentRepository

logger = logging.getLogger(__name__)


class Gateway:
    def __init__(
        self,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        rng: random.Random | None = None,
    ) -> None:
        self.sleep = sleep
        self.rng = rng or random.SystemRandom()

    async def process(self) -> str:
        await self.sleep(self.rng.uniform(2, 5))
        return "succeeded" if self.rng.random() < 0.9 else "failed"


class WebhookClient:
    def __init__(self, client: httpx.AsyncClient) -> None:
        self.client = client

    async def send(self, payment: Payment) -> None:
        response = await self.client.post(
            payment.webhook_url,
            json={
                "payment_id": str(payment.id),
                "status": payment.status,
                "amount": format(payment.amount, ".2f"),
                "currency": payment.currency,
                "processed_at": payment.processed_at.isoformat() if payment.processed_at else None,
            },
            headers={"Idempotency-Key": str(payment.id)},
        )
        response.raise_for_status()


class Processor:
    def __init__(
        self,
        repository: PaymentRepository,
        gateway: Gateway,
        webhook: WebhookClient,
        retry_base: float = 1,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self.repository = repository
        self.gateway = gateway
        self.webhook = webhook
        self.retry_base = retry_base
        self.sleep = sleep

    async def process(self, payment_id: UUID) -> None:
        payment = await self.repository.get(payment_id)
        if payment.status == "pending":
            status = await self.gateway.process()
            payment = await self.repository.finish(payment_id, status)
        if payment.webhook_delivered_at is not None:
            return
        await self.deliver(payment)

    async def deliver(self, payment: Payment) -> None:
        while (attempt := await self.repository.begin_attempt(payment.id)) is not None:
            if attempt > 1:
                await self.sleep(self.retry_base * 2 ** (attempt - 2))
            logger.info("Webhook payment=%s attempt=%s", payment.id, attempt)
            try:
                await self.webhook.send(payment)
            except httpx.HTTPError:
                # Do not log URLs/headers: the client's URL may contain credentials.
                logger.warning("Webhook delivery failed payment=%s attempt=%s", payment.id, attempt)
            else:
                await self.repository.delivered(payment.id)
                return
        raise DeliveryExhausted("Webhook delivery exhausted")
