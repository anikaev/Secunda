from unittest.mock import AsyncMock, MagicMock

import pytest
from httpx import AsyncClient

from payments_service.db.models import Payment
from payments_service.services.payments import IdempotencyConflict


@pytest.fixture
def body() -> dict:
    return {
        "amount": "1250.50",
        "currency": "RUB",
        "description": "Заказ 42",
        "metadata": {"order_id": 42},
        "webhook_url": "https://client.example/webhook",
    }


@pytest.fixture
def headers() -> dict:
    return {"X-API-Key": "test-api-key", "Idempotency-Key": "order-42"}


async def test_create_returns_202(
    client: AsyncClient,
    body: dict,
    headers: dict,
    payment: Payment,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    create = AsyncMock(return_value=payment)
    monkeypatch.setattr("payments_service.api.payments.create_payment", create)
    response = await client.post("/api/v1/payments", json=body, headers=headers)
    assert response.status_code == 202
    assert response.json()["payment_id"] == str(payment.id)
    assert response.json()["status"] == "pending"
    create.assert_awaited_once()


@pytest.mark.parametrize("key", [None, "", "   ", "x" * 256])
async def test_create_requires_idempotency_key(
    client: AsyncClient, body: dict, key: str | None
) -> None:
    headers = {"X-API-Key": "test-api-key"}
    if key is not None:
        headers["Idempotency-Key"] = key
    response = await client.post("/api/v1/payments", json=body, headers=headers)
    assert response.status_code == 422


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("amount", "0"),
        ("amount", "-1"),
        ("amount", "1.001"),
        ("amount", "10000000000000000.00"),
        ("amount", "NaN"),
        ("currency", "BTC"),
        ("webhook_url", "ftp://client.example/webhook"),
        ("webhook_url", "https://user:secret@client.example/webhook"),
        ("webhook_url", "https://client.example/webhook#fragment"),
        ("metadata", []),
    ],
)
async def test_create_rejects_invalid_body(
    client: AsyncClient, body: dict, headers: dict, field: str, value: object
) -> None:
    body[field] = value
    response = await client.post("/api/v1/payments", json=body, headers=headers)
    assert response.status_code == 422


async def test_create_returns_409_for_reused_key(
    client: AsyncClient, body: dict, headers: dict, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        "payments_service.api.payments.create_payment", AsyncMock(side_effect=IdempotencyConflict)
    )
    response = await client.post("/api/v1/payments", json=body, headers=headers)
    assert response.status_code == 409


async def test_get_serializes_decimal_and_metadata(
    client: AsyncClient, payment: Payment, db_session: MagicMock
) -> None:
    db_session.get.return_value = payment
    response = await client.get(
        f"/api/v1/payments/{payment.id}", headers={"X-API-Key": "test-api-key"}
    )
    assert response.status_code == 200
    assert response.json()["amount"] == "1250.50"
    assert response.json()["metadata"] == {"order_id": 42}
    assert response.json()["payment_id"] == str(payment.id)


async def test_get_unknown_payment_returns_404(
    client: AsyncClient, payment: Payment, db_session: MagicMock
) -> None:
    db_session.get.return_value = None
    response = await client.get(
        f"/api/v1/payments/{payment.id}", headers={"X-API-Key": "test-api-key"}
    )
    assert response.status_code == 404


@pytest.mark.parametrize("method", ["POST", "GET"])
async def test_payment_routes_require_api_key(
    client: AsyncClient, body: dict, payment: Payment, method: str
) -> None:
    path = "/api/v1/payments" if method == "POST" else f"/api/v1/payments/{payment.id}"
    response = await client.request(method, path, json=body)
    assert response.status_code == 401
