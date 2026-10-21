import asyncio
import json
import os
from uuid import uuid4

import aio_pika
import httpx
import pytest
from sqlalchemy import event, func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from payments_service.db.models import OutboxEvent, Payment
from payments_service.messaging import EXCHANGE, NEW_QUEUE
from payments_service.schemas import PaymentCreate
from payments_service.services.payments import create_payment
from tests.integration.helpers import (
    API_URL,
    AUTH,
    create,
    delivered,
    detail,
    eventually,
    new_hook,
    payment_body,
    requests,
)

pytestmark = pytest.mark.integration


async def test_api_authentication_and_validation(network_client: httpx.AsyncClient) -> None:
    client = network_client
    for path in ("/health", "/health/ready", f"/api/v1/payments/{uuid4()}"):
        for headers in ({}, {"X-API-Key": "wrong"}):
            assert (await client.get(f"{API_URL}{path}", headers=headers)).status_code == 401
    token = await new_hook(client)
    assert (
        await client.post(f"{API_URL}/api/v1/payments", json=payment_body(token))
    ).status_code == 401
    assert (
        await client.post(f"{API_URL}/api/v1/payments", headers=AUTH, json=payment_body(token))
    ).status_code == 422
    for field, value in (("amount", "1.001"), ("currency", "BTC")):
        body = {**payment_body(token), field: value}
        response = await client.post(
            f"{API_URL}/api/v1/payments",
            headers={**AUTH, "Idempotency-Key": token},
            json=body,
        )
        assert response.status_code == 422
    assert (
        await client.get(f"{API_URL}/api/v1/payments/{uuid4()}", headers=AUTH)
    ).status_code == 404


async def test_concurrent_creation_and_duplicate_event(
    network_client: httpx.AsyncClient, engine: AsyncEngine
) -> None:
    client = network_client
    token = await new_hook(client)
    responses = await asyncio.gather(*(create(client, token) for _ in range(8)))
    ids = {response["payment_id"] for response in responses}
    assert len(ids) == 1
    payment_id = ids.pop()
    payment = await eventually(lambda: delivered(client, payment_id))
    assert payment["amount"] == "1250.50"
    assert payment["metadata"] == {"test": token}
    assert payment["status"] in ("succeeded", "failed")
    assert payment["processed_at"] is not None
    assert (await create(client, token))["payment_id"] == payment_id
    response = await client.post(
        f"{API_URL}/api/v1/payments",
        headers={**AUTH, "Idempotency-Key": token},
        json={**payment_body(token), "amount": "99.00"},
    )
    assert response.status_code == 409
    async with AsyncSession(engine) as session:
        assert (
            await session.scalar(
                select(func.count()).select_from(Payment).where(Payment.idempotency_key == token)
            )
            == 1
        )
        events = (
            await session.scalars(
                select(OutboxEvent).where(
                    OutboxEvent.payload["payment_id"].astext == payment_id,
                    OutboxEvent.topic == NEW_QUEUE,
                )
            )
        ).all()
        assert len(events) == 1
        event = events[0]
        assert event.published_at is not None
        payload = event.payload
    before = await requests(client, token)
    assert len(before) == 1
    hook = before[0]
    assert hook["payload"]["status"] == payment["status"]
    assert hook["payload"]["amount"] == "1250.50"
    assert hook["payload"]["event_type"] == "payment.processed"
    assert hook["event_id"] == hook["idempotency_key"] == payload["event_id"]

    # Real AMQP redelivery, followed by a sentinel on the same FIFO queue.
    connection = await aio_pika.connect_robust(os.environ["RABBITMQ_URL"])
    async with connection:
        channel = await connection.channel(publisher_confirms=True)
        exchange = await channel.get_exchange(EXCHANGE.name)
        await exchange.publish(
            aio_pika.Message(json.dumps(payload).encode()), routing_key=NEW_QUEUE
        )
    sentinel = await new_hook(client)
    accepted = await create(client, sentinel)
    await eventually(lambda: delivered(client, accepted["payment_id"]))
    assert await requests(client, token) == before
    after = await detail(client, payment_id)
    assert after["processed_at"] == payment["processed_at"]


async def test_webhook_retry_uses_broker_ttl(network_client: httpx.AsyncClient) -> None:
    client = network_client
    token = await new_hook(client, statuses=[503, 503, 204])
    accepted = await create(client, token)
    await eventually(lambda: delivered(client, accepted["payment_id"]))
    attempts = await requests(client, token)
    assert len(attempts) == 3
    assert attempts[1]["time"] - attempts[0]["time"] >= 0.9
    assert attempts[2]["time"] - attempts[1]["time"] >= 1.9
    assert all(item["payload"] == attempts[0]["payload"] for item in attempts)
    assert all(item["event_id"] == item["idempotency_key"] for item in attempts)


async def test_third_failure_is_delivered_to_real_dlq(network_client: httpx.AsyncClient) -> None:
    client = network_client
    token = await new_hook(client, statuses=[503])
    accepted = await create(client, token)
    connection = await aio_pika.connect_robust(os.environ["RABBITMQ_URL"])
    async with connection:
        channel = await connection.channel()
        queue = await channel.get_queue("payments.dead")

        async def dead_message() -> dict | None:
            message = await queue.get(fail=False)
            if message is None:
                return None
            async with message.process():
                payload = json.loads(message.body)
                assert payload["payment_id"] == accepted["payment_id"]
                return payload

        dead = await eventually(dead_message)
    assert dead["attempt"] == 3
    assert dead["error_type"] == "HTTPStatusError"
    assert len(await requests(client, token)) == 3
    payment = await detail(client, accepted["payment_id"])
    assert payment["status"] in ("succeeded", "failed")
    assert payment["processed_at"] is not None
    assert payment["webhook_delivered_at"] is None


async def test_malformed_message_is_dead_lettered() -> None:
    connection = await aio_pika.connect_robust(os.environ["RABBITMQ_URL"])
    async with connection:
        channel = await connection.channel(publisher_confirms=True)
        exchange = await channel.get_exchange(EXCHANGE.name)
        body = b"invalid-payment-json"
        await exchange.publish(aio_pika.Message(body), routing_key=NEW_QUEUE)
        queue = await channel.get_queue("payments.dead")

        async def dead_message() -> bool:
            message = await queue.get(fail=False)
            if message is None:
                return False
            async with message.process():
                assert message.body == body
                assert message.headers["x-first-death-reason"] == "rejected"
                return True

        await eventually(dead_message)


async def test_payment_and_outbox_are_rolled_back_together(engine: AsyncEngine) -> None:
    token = uuid4().hex
    outbox_ids = []

    # Invalid transaction-local topic forces a real outbox constraint failure at commit.
    def corrupt_outbox(session, flush_context, instances):
        for obj in session.new:
            if isinstance(obj, OutboxEvent):
                outbox_ids.append(obj.id)
                obj.topic = None

    async with AsyncSession(engine, expire_on_commit=False) as session:
        event.listen(session.sync_session, "before_flush", corrupt_outbox)
        with pytest.raises(IntegrityError):
            await create_payment(session, PaymentCreate(**payment_body(token)), token)
    async with AsyncSession(engine) as session:
        assert (
            await session.scalar(
                select(func.count()).select_from(Payment).where(Payment.idempotency_key == token)
            )
            == 0
        )
        assert len(outbox_ids) == 1
        assert await session.get(OutboxEvent, outbox_ids[0]) is None
