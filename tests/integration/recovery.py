import argparse
import asyncio
import json
import os
from pathlib import Path

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from payments_service.db.models import OutboxEvent
from tests.integration.helpers import (
    API_HEADERS,
    API_URL,
    WEBHOOK_URL,
    create_webhook,
    get_payment,
    get_webhook_requests,
    submit_payment,
    wait_for_webhook,
    wait_until,
)

STATE_FILE = Path("/state/recovery.json")


async def check_unpublished_event(payment_id: str) -> None:
    engine = create_async_engine(os.environ["DATABASE_URL"])
    try:
        async with AsyncSession(engine) as session:
            result = await session.scalars(
                select(OutboxEvent).where(OutboxEvent.payload["payment_id"].astext == payment_id)
            )
            events = result.all()
            assert len(events) == 1
            assert events[0].published_at is None
    finally:
        await engine.dispose()


async def create_pending_payment(client: httpx.AsyncClient) -> dict:
    webhook_id = await create_webhook(client)
    accepted = await submit_payment(client, webhook_id)
    payment = await get_payment(client, accepted["payment_id"])
    assert payment["status"] == "pending"
    assert payment["processed_at"] is None
    await check_unpublished_event(payment["payment_id"])
    return {"webhook_id": webhook_id, "payment": payment}


async def prepare_interrupted_webhook(client: httpx.AsyncClient) -> dict:
    webhook_id = await create_webhook(client, hold=True)
    accepted = await submit_payment(client, webhook_id)
    await wait_until(lambda: get_webhook_requests(client, webhook_id))

    payment = await get_payment(client, accepted["payment_id"])
    assert payment["processed_at"] is not None
    assert payment["webhook_delivered_at"] is None
    return {"webhook_id": webhook_id, "payment": payment}


async def release_webhook(client: httpx.AsyncClient, state: dict) -> None:
    response = await client.post(f"{WEBHOOK_URL}/control/{state['webhook_id']}/release")
    response.raise_for_status()


async def check_delivery(
    client: httpx.AsyncClient, state: dict, *, after_crash: bool = False
) -> None:
    original_payment = state["payment"]
    payment_id = original_payment["payment_id"]
    payment = await wait_for_webhook(client, payment_id, seconds=60)
    repeated = await submit_payment(client, state["webhook_id"])
    assert repeated["payment_id"] == payment_id

    received = await get_webhook_requests(client, state["webhook_id"])
    expected_requests = 2 if after_crash else 1
    assert len(received) == expected_requests, received

    if after_crash:
        assert payment["status"] == original_payment["status"]
        assert payment["processed_at"] == original_payment["processed_at"]
        assert received[0]["payload"] == received[1]["payload"]
        assert received[0]["event_id"] == received[1]["event_id"]


async def check_database_outage(client: httpx.AsyncClient) -> None:
    response = await client.get(f"{API_URL}/health/ready", headers=API_HEADERS)
    assert response.status_code == 503, response.text
    assert response.json() == {"detail": "Database is unavailable"}

    response = await client.get(f"{API_URL}/health", headers=API_HEADERS)
    assert response.status_code == 200


async def check_readiness(client: httpx.AsyncClient) -> None:
    response = await client.get(f"{API_URL}/health/ready", headers=API_HEADERS)
    assert response.status_code == 200, response.text


async def check_payment_flow(client: httpx.AsyncClient) -> None:
    webhook_id = await create_webhook(client)
    accepted = await submit_payment(client, webhook_id)
    await wait_for_webhook(client, accepted["payment_id"])
    received = await get_webhook_requests(client, webhook_id)
    assert len(received) == 1


async def run_step(step: str, state: dict) -> dict | None:
    async with httpx.AsyncClient(timeout=10, trust_env=False) as client:
        match step:
            case "create-pending":
                return await create_pending_payment(client)
            case "prepare-crash":
                return await prepare_interrupted_webhook(client)
            case "release-webhook":
                await release_webhook(client, state)
            case "check-delivery":
                await check_delivery(client, state)
            case "check-redelivery":
                await check_delivery(client, state, after_crash=True)
            case "check-db-outage":
                await check_database_outage(client)
            case "check-readiness":
                await check_readiness(client)
            case "check-payment":
                await check_payment_flow(client)
            case _:
                raise ValueError(f"Неизвестный шаг: {step}")
    return None


def main() -> None:
    parser = argparse.ArgumentParser(description="Проверка восстановления платёжного сервиса")
    parser.add_argument(
        "step",
        choices=(
            "create-pending",
            "prepare-crash",
            "release-webhook",
            "check-delivery",
            "check-redelivery",
            "check-db-outage",
            "check-readiness",
            "check-payment",
        ),
        help="Шаг сценария из scripts/test-compose.sh",
    )
    step = parser.parse_args().step
    state = json.loads(STATE_FILE.read_text()) if STATE_FILE.exists() else {}
    new_state = asyncio.run(run_step(step, state))
    if new_state is not None:
        STATE_FILE.write_text(json.dumps(new_state))
    print(f"Проверка выполнена: {step}", flush=True)


if __name__ == "__main__":
    main()
