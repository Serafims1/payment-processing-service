from unittest.mock import AsyncMock
from uuid import uuid4

import httpx
import pytest

from payments.api import create_app
from payments.exceptions import IdempotencyConflict, PaymentNotFound
from payments.models import Payment, utcnow
from payments.service import PaymentService


@pytest.fixture
async def api(settings, payment_request):
    app = create_app(settings)
    service = AsyncMock(spec=PaymentService)
    item = Payment(
        id=uuid4(),
        amount=payment_request.amount,
        currency="RUB",
        description="Order",
        metadata_json={"order": 1},
        status="pending",
        created_at=utcnow(),
        processed_at=None,
        webhook_url=str(payment_request.webhook_url),
    )
    service.create.return_value = item
    service.get.return_value = item
    app.state.service = service
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        yield client, service, item


async def test_post_get_and_duplicate(api, payment_request):
    client, service, item = api
    headers = {"X-API-Key": "test-api-key", "Idempotency-Key": "key"}
    response = await client.post(
        "/api/v1/payments", headers=headers, json=payment_request.model_dump(mode="json")
    )
    assert response.status_code == 202
    assert response.json()["status"] == "pending"
    assert response.json()["payment_id"] == str(item.id)
    duplicate = await client.post(
        "/api/v1/payments", headers=headers, json=payment_request.model_dump(mode="json")
    )
    assert duplicate.json() == response.json()
    result = await client.get(f"/api/v1/payments/{item.id}", headers=headers)
    assert result.status_code == 200
    assert result.json()["amount"] == "12.30"
    assert result.json()["metadata"] == {"order": 1}
    service.create.assert_awaited()


@pytest.mark.parametrize("key", [None, "bad", "тест"])
@pytest.mark.parametrize("method", ["post", "get"])
async def test_api_key_required(api, payment_request, key, method):
    client, _, item = api
    headers = {"Idempotency-Key": "key"}
    if key is not None:
        if not key.isascii():
            # httpx rejects non-ASCII headers before the application boundary.
            headers["X-API-Key"] = key.encode("utf8")
        else:
            headers["X-API-Key"] = key
    if method == "post":
        response = await client.post(
            "/api/v1/payments", headers=headers, json=payment_request.model_dump(mode="json")
        )
    else:
        response = await client.get(f"/api/v1/payments/{item.id}", headers=headers)
    assert response.status_code == 401


async def test_missing_idempotency_key(api, payment_request):
    client, _, _ = api
    result = await client.post(
        "/api/v1/payments",
        headers={"X-API-Key": "test-api-key"},
        json=payment_request.model_dump(mode="json"),
    )
    assert result.status_code == 422


@pytest.mark.parametrize(
    "change",
    [
        {"amount": "-1"},
        {"amount": 1.2},
        {"currency": "BTC"},
        {"webhook_url": "ftp://test/file"},
        {"webhook_url": "bad"},
        {"unknown": 1},
    ],
)
async def test_invalid_body(api, payment_request, change):
    client, _, _ = api
    result = await client.post(
        "/api/v1/payments",
        headers={"X-API-Key": "test-api-key", "Idempotency-Key": "key"},
        json=payment_request.model_dump(mode="json") | change,
    )
    assert result.status_code == 422


async def test_not_found_and_conflict(api, payment_request):
    client, service, item = api
    headers = {"X-API-Key": "test-api-key", "Idempotency-Key": "key"}
    service.get.side_effect = PaymentNotFound("Payment not found")
    assert (await client.get(f"/api/v1/payments/{item.id}", headers=headers)).status_code == 404
    service.create.side_effect = IdempotencyConflict("Conflict")
    assert (
        await client.post(
            "/api/v1/payments", headers=headers, json=payment_request.model_dump(mode="json")
        )
    ).status_code == 409


async def test_unexpected_error_is_sanitized(settings, payment_request):
    app = create_app(settings)
    app.state.service = AsyncMock(spec=PaymentService)
    app.state.service.create.side_effect = RuntimeError("private internal traceback")
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app, raise_app_exceptions=False), base_url="http://test"
    ) as client:
        result = await client.post(
            "/api/v1/payments",
            headers={"X-API-Key": "test-api-key", "Idempotency-Key": "key"},
            json=payment_request.model_dump(mode="json"),
        )
    assert result.status_code == 500
    assert result.json() == {"detail": "Internal server error"}
