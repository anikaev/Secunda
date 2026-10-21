import asyncio
import os
from collections.abc import Awaitable, Callable
from uuid import uuid4

import httpx

API_URL = "http://api:8000"
WEBHOOK_URL = "http://webhook:8080"
AUTH = {"X-API-Key": os.environ.get("API_KEY", "integration-key")}


async def eventually[T](check: Callable[[], Awaitable[T]], *, seconds: float = 30) -> T:
    try:
        async with asyncio.timeout(seconds):
            while True:
                value = await check()
                if value:
                    return value
                await asyncio.sleep(0.1)
    except TimeoutError as exc:
        raise AssertionError(f"Condition did not become true within {seconds} seconds") from exc


async def new_hook(client: httpx.AsyncClient, **behavior: object) -> str:
    token = uuid4().hex
    response = await client.put(f"{WEBHOOK_URL}/control/{token}", json=behavior)
    response.raise_for_status()
    return token


def payment_body(token: str) -> dict:
    return {
        "amount": "1250.50",
        "currency": "RUB",
        "description": "Integration order",
        "metadata": {"test": token},
        "webhook_url": f"{WEBHOOK_URL}/hooks/{token}",
    }


async def create(client: httpx.AsyncClient, token: str, key: str | None = None) -> dict:
    response = await client.post(
        f"{API_URL}/api/v1/payments",
        headers={**AUTH, "Idempotency-Key": key or token},
        json=payment_body(token),
    )
    assert response.status_code == 202, response.text
    return response.json()


async def detail(client: httpx.AsyncClient, payment_id: str) -> dict:
    response = await client.get(f"{API_URL}/api/v1/payments/{payment_id}", headers=AUTH)
    response.raise_for_status()
    return response.json()


async def requests(client: httpx.AsyncClient, token: str) -> list[dict]:
    response = await client.get(f"{WEBHOOK_URL}/control/{token}")
    response.raise_for_status()
    return response.json()


async def delivered(client: httpx.AsyncClient, payment_id: str) -> dict | None:
    payment = await detail(client, payment_id)
    return payment if payment["webhook_delivered_at"] else None
