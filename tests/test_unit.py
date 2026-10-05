import asyncio
import random
from decimal import Decimal
from unittest.mock import AsyncMock
from uuid import uuid4

import httpx
import pytest
from pydantic import ValidationError

from payments.exceptions import DeliveryExhausted, IdempotencyConflict
from payments.models import Payment, utcnow
from payments.processing import Gateway, Processor, WebhookClient
from payments.repositories import PaymentRepository
from payments.schemas import PaymentCreate
from payments.service import PaymentService


def payment(status="pending"):
    return Payment(
        id=uuid4(),
        amount=Decimal("12.30"),
        currency="RUB",
        status=status,
        webhook_url="http://client.test/hook",
        processed_at=utcnow(),
        webhook_attempts=0,
        webhook_delivered_at=None,
    )


@pytest.mark.parametrize(
    "draw,status", [(0.1, "succeeded"), (0.89, "succeeded"), (0.9, "failed"), (0.99, "failed")]
)
async def test_gateway(draw, status):
    rng = random.Random(1)
    rng.random = lambda: draw
    sleep = AsyncMock()
    assert await Gateway(sleep, rng).process() == status
    assert 2 <= sleep.call_args.args[0] <= 5


async def test_service_delegates_and_propagates_conflict(payment_request):
    repo = AsyncMock(spec=PaymentRepository)
    instance = payment()
    repo.create.return_value = instance
    repo.get.return_value = instance
    service = PaymentService(repo)
    assert await service.create("key", payment_request) is instance
    assert await service.get(instance.id) is instance
    repo.create.side_effect = IdempotencyConflict()
    with pytest.raises(IdempotencyConflict):
        await service.create("key", payment_request)


@pytest.mark.parametrize("terminal", ["succeeded", "failed"])
async def test_terminal_delivery_never_reprocesses_gateway(terminal):
    repo = AsyncMock(spec=PaymentRepository)
    item = payment(terminal)
    repo.get.return_value = item
    repo.begin_attempt.side_effect = [1]
    gateway = AsyncMock(spec=Gateway)
    webhook = AsyncMock(spec=WebhookClient)
    await Processor(repo, gateway, webhook).process(item.id)
    gateway.process.assert_not_awaited()
    repo.finish.assert_not_awaited()
    webhook.send.assert_awaited_once_with(item)


async def test_delivered_duplicate_is_noop():
    repo = AsyncMock(spec=PaymentRepository)
    item = payment("succeeded")
    item.webhook_delivered_at = utcnow()
    repo.get.return_value = item
    gateway, webhook = AsyncMock(spec=Gateway), AsyncMock(spec=WebhookClient)
    await Processor(repo, gateway, webhook).process(item.id)
    gateway.process.assert_not_awaited()
    webhook.send.assert_not_awaited()
    repo.begin_attempt.assert_not_awaited()


@pytest.mark.parametrize("failures", [0, 1, 2, 3])
async def test_retries_and_persisted_attempt_budget(failures):
    repo = AsyncMock(spec=PaymentRepository)
    item = payment()
    final = payment("succeeded")
    repo.get.return_value = item
    repo.finish.return_value = final
    repo.begin_attempt.side_effect = [1, 2, 3, None]
    gateway, webhook = AsyncMock(spec=Gateway), AsyncMock(spec=WebhookClient)
    gateway.process.return_value = "succeeded"
    webhook.send.side_effect = [httpx.ConnectError("unavailable")] * failures + [None]
    sleep = AsyncMock()
    processor = Processor(repo, gateway, webhook, retry_base=2, sleep=sleep)
    if failures == 3:
        with pytest.raises(DeliveryExhausted):
            await processor.process(item.id)
        repo.delivered.assert_not_awaited()
    else:
        await processor.process(item.id)
        repo.delivered.assert_awaited_once()
    gateway.process.assert_awaited_once()
    assert webhook.send.await_count == min(failures + 1, 3)
    assert [call.args[0] for call in sleep.call_args_list] == [2, 4][: min(failures, 2)]


async def test_restart_with_exhausted_attempts_does_not_send():
    repo = AsyncMock(spec=PaymentRepository)
    repo.get.return_value = payment("failed")
    repo.begin_attempt.return_value = None
    webhook = AsyncMock(spec=WebhookClient)
    with pytest.raises(DeliveryExhausted):
        await Processor(repo, AsyncMock(spec=Gateway), webhook).process(uuid4())
    webhook.send.assert_not_awaited()


@pytest.mark.parametrize("code", [200, 204, 302, 400, 500])
async def test_webhook_http_status_and_decimal_payload(code):
    item = payment("succeeded")
    requests = []

    def respond(request):
        requests.append(request)
        return httpx.Response(code)

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond), timeout=1) as client:
        if code >= 300:
            with pytest.raises(httpx.HTTPStatusError):
                await WebhookClient(client).send(item)
        else:
            await WebhookClient(client).send(item)
    import json

    body = json.loads(requests[0].content)
    assert body["amount"] == "12.30"
    assert body["payment_id"] == str(item.id)
    assert requests[0].headers["Idempotency-Key"] == str(item.id)


@pytest.mark.parametrize(
    "amount", ["0", "-1", "NaN", "Infinity", "1.001", 1.2, True, "1000000000000000000.00"]
)
def test_invalid_amount(amount, payment_request):
    with pytest.raises(ValidationError):
        PaymentCreate.model_validate(payment_request.model_dump() | {"amount": amount})


def test_canonical_decimal_and_metadata(payment_request):
    other = PaymentCreate.model_validate(payment_request.model_dump() | {"amount": "12.3"})
    assert other.canonical_payload() == payment_request.canonical_payload()


async def test_gateway_cancellation():
    sleep = AsyncMock(side_effect=asyncio.CancelledError)
    with pytest.raises(asyncio.CancelledError):
        await Gateway(sleep).process()
