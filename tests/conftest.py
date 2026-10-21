from collections.abc import AsyncIterator
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from payments_service.config import Settings
from payments_service.db.models import Currency, Payment, PaymentStatus
from payments_service.db.session import get_session
from payments_service.main import create_app
from payments_service.schemas import PaymentEvent


@pytest.fixture
def settings() -> Settings:
    return Settings(
        _env_file=None,
        api_key="test-api-key",
        database_url="postgresql+asyncpg://test:test@localhost/test",
        rabbitmq_url="amqp://test:test@localhost/",
    )


@pytest.fixture
def app(settings: Settings) -> FastAPI:
    return create_app(settings)


@pytest.fixture
async def client(app: FastAPI) -> AsyncIterator[AsyncClient]:
    async with app.router.lifespan_context(app):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            yield client


@pytest.fixture
def payment() -> Payment:
    return Payment(
        id=uuid4(),
        amount=Decimal("1250.50"),
        currency=Currency.RUB,
        description="Заказ 42",
        metadata_={"order_id": 42},
        status=PaymentStatus.PENDING,
        idempotency_key="order-42",
        webhook_url="https://client.example/webhook",
        created_at=datetime.now(UTC),
        processed_at=None,
        webhook_delivered_at=None,
    )


@pytest.fixture
def event(payment: Payment) -> PaymentEvent:
    return PaymentEvent(event_id=uuid4(), payment_id=payment.id)


@pytest.fixture
def session(payment: Payment) -> MagicMock:
    session = MagicMock(spec=AsyncSession)
    session.scalar.return_value = payment
    session.get.return_value = None
    return session


@pytest.fixture
def db_session(app: FastAPI, session: MagicMock) -> MagicMock:
    async def override_session() -> AsyncIterator[MagicMock]:
        yield session

    app.dependency_overrides[get_session] = override_session
    return session


@pytest.fixture
def sessions(session: MagicMock) -> MagicMock:
    sessions = MagicMock()
    sessions.begin.return_value.__aenter__ = AsyncMock(return_value=session)
    sessions.begin.return_value.__aexit__ = AsyncMock(return_value=False)
    return sessions
