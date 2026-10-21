import asyncio
import os
from collections.abc import Awaitable, Callable
from uuid import uuid4

import httpx
from aio_pika.abc import AbstractIncomingMessage, AbstractQueue

API_URL = "http://api:8000"
WEBHOOK_URL = "http://webhook:8080"
API_HEADERS = {"X-API-Key": os.environ.get("API_KEY", "integration-key")}


async def wait_until[T](check: Callable[[], Awaitable[T]], *, seconds: float = 30) -> T:
    try:
        async with asyncio.timeout(seconds):
            while True:
                result = await check()
                if result:
                    return result
                await asyncio.sleep(0.1)
    except TimeoutError as exc:
        raise AssertionError(f"Условие не выполнилось за {seconds} секунд") from exc


async def create_webhook(
    client: httpx.AsyncClient, *, statuses: list[int] | None = None, hold: bool = False
) -> str:
    webhook_id = uuid4().hex
    response = await client.put(
        f"{WEBHOOK_URL}/control/{webhook_id}",
        json={"statuses": statuses if statuses is not None else [204], "hold": hold},
    )
    response.raise_for_status()
    return webhook_id


def payment_data(webhook_id: str) -> dict:
    return {
        "amount": "1250.50",
        "currency": "RUB",
        "description": "Тестовый заказ",
        "metadata": {"test": webhook_id},
        "webhook_url": f"{WEBHOOK_URL}/hooks/{webhook_id}",
    }


async def submit_payment(
    client: httpx.AsyncClient, webhook_id: str, *, idempotency_key: str | None = None
) -> dict:
    response = await client.post(
        f"{API_URL}/api/v1/payments",
        headers={**API_HEADERS, "Idempotency-Key": idempotency_key or webhook_id},
        json=payment_data(webhook_id),
    )
    assert response.status_code == 202, response.text
    return response.json()


async def get_payment(client: httpx.AsyncClient, payment_id: str) -> dict:
    response = await client.get(f"{API_URL}/api/v1/payments/{payment_id}", headers=API_HEADERS)
    response.raise_for_status()
    return response.json()


async def get_webhook_requests(client: httpx.AsyncClient, webhook_id: str) -> list[dict]:
    response = await client.get(f"{WEBHOOK_URL}/control/{webhook_id}")
    response.raise_for_status()
    return response.json()


async def wait_for_webhook(
    client: httpx.AsyncClient, payment_id: str, *, seconds: float = 30
) -> dict:
    async def check_delivery() -> dict | None:
        payment = await get_payment(client, payment_id)
        return payment if payment["webhook_delivered_at"] else None

    return await wait_until(check_delivery, seconds=seconds)


async def wait_for_dead_message(queue: AbstractQueue) -> AbstractIncomingMessage:
    message = await wait_until(lambda: queue.get(fail=False))
    await message.ack()
    return message
