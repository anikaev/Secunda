"""Acceptance checks run around actual service stops by scripts/test-compose.sh."""

import asyncio
import json
import os
import sys
from pathlib import Path

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from payments_service.db.models import OutboxEvent
from tests.integration.helpers import (
    API_URL,
    AUTH,
    WEBHOOK_URL,
    create,
    delivered,
    detail,
    eventually,
    new_hook,
    requests,
)

STATE = Path("/state/recovery.json")


async def check_unpublished(payment_id: str) -> None:
    engine = create_async_engine(os.environ["DATABASE_URL"])
    try:
        async with AsyncSession(engine) as session:
            rows = (
                await session.scalars(
                    select(OutboxEvent).where(
                        OutboxEvent.payload["payment_id"].astext == payment_id
                    )
                )
            ).all()
            assert len(rows) == 1
            assert rows[0].published_at is None
    finally:
        await engine.dispose()


async def run(mode: str) -> None:
    async with httpx.AsyncClient(timeout=10, trust_env=False) as client:
        if mode == "seed":
            token = await new_hook(client)
            accepted = await create(client, token)
            payment = await detail(client, accepted["payment_id"])
            assert payment["status"] == "pending"
            assert payment["processed_at"] is None
            await check_unpublished(accepted["payment_id"])
            await asyncio.to_thread(
                STATE.write_text, json.dumps({"token": token, "payment": accepted})
            )
        elif mode == "prepare-crash":
            token = await new_hook(client, hold=True)
            accepted = await create(client, token)
            await eventually(lambda: requests(client, token))
            payment = await detail(client, accepted["payment_id"])
            assert payment["processed_at"] is not None
            assert payment["webhook_delivered_at"] is None
            await asyncio.to_thread(
                STATE.write_text, json.dumps({"token": token, "payment": payment})
            )
        elif mode == "release":
            state = json.loads(await asyncio.to_thread(STATE.read_text))
            response = await client.post(f"{WEBHOOK_URL}/control/{state['token']}/release")
            response.raise_for_status()
        elif mode in ("verify", "verify-crash"):
            state = json.loads(await asyncio.to_thread(STATE.read_text))
            payment_id = state["payment"]["payment_id"]
            payment = await eventually(lambda: delivered(client, payment_id), seconds=60)
            assert (await create(client, state["token"]))["payment_id"] == payment_id
            seen = await requests(client, state["token"])
            assert len(seen) == (2 if mode == "verify-crash" else 1), seen
            if mode == "verify-crash":
                assert payment["status"] == state["payment"]["status"]
                assert payment["processed_at"] == state["payment"]["processed_at"]
                assert seen[0]["payload"] == seen[1]["payload"]
                assert seen[0]["event_id"] == seen[1]["event_id"]
        elif mode == "db-down":
            response = await client.get(f"{API_URL}/health/ready", headers=AUTH)
            assert response.status_code == 503, response.text
            assert response.json() == {"detail": "Database is unavailable"}
            assert (await client.get(f"{API_URL}/health", headers=AUTH)).status_code == 200
        elif mode == "healthy":
            response = await client.get(f"{API_URL}/health/ready", headers=AUTH)
            assert response.status_code == 200, response.text
        elif mode == "smoke":
            token = await new_hook(client)
            accepted = await create(client, token)
            await eventually(lambda: delivered(client, accepted["payment_id"]))
            assert len(await requests(client, token)) == 1
        else:
            raise ValueError(mode)
    print(f"PASS: {mode}", flush=True)


if __name__ == "__main__":
    asyncio.run(run(sys.argv[1]))
