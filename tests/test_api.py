from collections.abc import AsyncIterator

import pytest
from asyncpg import InvalidPasswordError
from fastapi import FastAPI
from httpx import AsyncClient
from sqlalchemy.exc import OperationalError

from payments_service.db.session import get_session


@pytest.mark.parametrize("path", ["/health", "/health/ready"])
@pytest.mark.parametrize("key", [None, "wrong-key", "неверный-ключ"])
async def test_health_requires_valid_api_key(
    client: AsyncClient, path: str, key: str | None
) -> None:
    headers = {} if key is None else {b"X-API-Key": key.encode()}
    response = await client.get(path, headers=headers)
    assert response.status_code == 401


async def test_authenticated_liveness_does_not_require_database(client: AsyncClient) -> None:
    response = await client.get("/health", headers={"X-API-Key": "test-api-key"})
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


@pytest.mark.parametrize(
    "error",
    [
        OperationalError("SELECT 1", {}, Exception("private-connection-details")),
        ConnectionRefusedError("private-connection-details"),
        InvalidPasswordError("private-connection-details"),
        TimeoutError("private-connection-details"),
    ],
)
async def test_readiness_returns_503_without_exposing_database_error(
    app: FastAPI, client: AsyncClient, error: Exception
) -> None:
    class UnavailableSession:
        async def execute(self, statement: object) -> None:
            raise error

    async def unavailable_session() -> AsyncIterator[UnavailableSession]:
        yield UnavailableSession()

    app.dependency_overrides[get_session] = unavailable_session
    response = await client.get("/health/ready", headers={"X-API-Key": "test-api-key"})
    assert response.status_code == 503
    assert response.json() == {"detail": "Database is unavailable"}
