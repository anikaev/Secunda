from collections.abc import AsyncIterator

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from payments_service.core.config import Settings
from payments_service.main import create_app


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
