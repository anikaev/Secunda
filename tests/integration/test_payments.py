import asyncio
import json
from uuid import uuid4

import aio_pika
import httpx
import pytest
from aio_pika.abc import AbstractChannel, AbstractQueue
from sqlalchemy import event, func, select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from payments_service.db.models import OutboxEvent, Payment
from payments_service.messaging import EXCHANGE, NEW_QUEUE
from payments_service.schemas import PaymentCreate
from payments_service.services.payments import create_payment
from tests.integration.helpers import (
    API_HEADERS,
    API_URL,
    create_webhook,
    get_payment,
    get_webhook_requests,
    payment_data,
    submit_payment,
    wait_for_dead_message,
    wait_for_webhook,
    wait_until,
)

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("path", ["/health", "/health/ready", f"/api/v1/payments/{uuid4()}"])
@pytest.mark.parametrize("api_key", [None, "wrong"])
async def test_get_requires_api_key(
    client: httpx.AsyncClient, path: str, api_key: str | None
) -> None:
    headers = {"X-API-Key": api_key} if api_key else {}
    response = await client.get(f"{API_URL}{path}", headers=headers)
    assert response.status_code == 401


async def test_create_requires_api_key(client: httpx.AsyncClient) -> None:
    response = await client.post(f"{API_URL}/api/v1/payments", json=payment_data(uuid4().hex))
    assert response.status_code == 401


async def test_create_requires_idempotency_key(client: httpx.AsyncClient) -> None:
    response = await client.post(
        f"{API_URL}/api/v1/payments", headers=API_HEADERS, json=payment_data(uuid4().hex)
    )
    assert response.status_code == 422


@pytest.mark.parametrize(("field", "value"), [("amount", "1.001"), ("currency", "BTC")])
async def test_create_rejects_invalid_body(
    client: httpx.AsyncClient, field: str, value: str
) -> None:
    webhook_id = uuid4().hex
    body = payment_data(webhook_id)
    body[field] = value

    response = await client.post(
        f"{API_URL}/api/v1/payments",
        headers={**API_HEADERS, "Idempotency-Key": webhook_id},
        json=body,
    )
    assert response.status_code == 422


async def test_unknown_payment_returns_404(client: httpx.AsyncClient) -> None:
    response = await client.get(f"{API_URL}/api/v1/payments/{uuid4()}", headers=API_HEADERS)
    assert response.status_code == 404


async def test_concurrent_requests_create_one_payment(
    client: httpx.AsyncClient, engine: AsyncEngine
) -> None:
    webhook_id = await create_webhook(client)
    responses = await asyncio.gather(*(submit_payment(client, webhook_id) for _ in range(8)))
    payment_ids = {response["payment_id"] for response in responses}
    assert len(payment_ids) == 1

    payment_id = responses[0]["payment_id"]
    payment = await wait_for_webhook(client, payment_id)
    repeated = await submit_payment(client, webhook_id)
    assert repeated["payment_id"] == payment_id

    async with AsyncSession(engine) as session:
        count = await session.scalar(
            select(func.count()).select_from(Payment).where(Payment.idempotency_key == webhook_id)
        )
        assert count == 1

        result = await session.scalars(
            select(OutboxEvent).where(
                OutboxEvent.payload["payment_id"].astext == payment_id,
                OutboxEvent.topic == NEW_QUEUE,
            )
        )
        events = result.all()
        assert len(events) == 1
        assert events[0].published_at is not None
        event_id = str(events[0].id)

    received = await get_webhook_requests(client, webhook_id)
    assert len(received) == 1
    notification = received[0]
    payload = notification["payload"]
    assert payment["amount"] == payload["amount"] == "1250.50"
    assert payment["metadata"] == {"test": webhook_id}
    assert payment["status"] in ("succeeded", "failed")
    assert payment["processed_at"] is not None
    assert payload["payment_id"] == payment_id
    assert payload["status"] == payment["status"]
    assert payload["event_type"] == "payment.processed"
    assert notification["event_id"] == notification["idempotency_key"] == payload["event_id"]
    assert notification["event_id"] == event_id


async def test_reusing_key_with_different_body_returns_409(client: httpx.AsyncClient) -> None:
    webhook_id = await create_webhook(client)
    accepted = await submit_payment(client, webhook_id)
    body = payment_data(webhook_id)
    body["amount"] = "99.00"

    response = await client.post(
        f"{API_URL}/api/v1/payments",
        headers={**API_HEADERS, "Idempotency-Key": webhook_id},
        json=body,
    )
    assert response.status_code == 409
    await wait_for_webhook(client, accepted["payment_id"])


async def test_duplicate_event_does_not_repeat_payment_or_webhook(
    client: httpx.AsyncClient, engine: AsyncEngine, rabbit_channel: AbstractChannel
) -> None:
    webhook_id = await create_webhook(client)
    accepted = await submit_payment(client, webhook_id)
    payment_id = accepted["payment_id"]
    payment_before = await wait_for_webhook(client, payment_id)
    requests_before = await get_webhook_requests(client, webhook_id)
    assert len(requests_before) == 1

    async with AsyncSession(engine) as session:
        original_event = await session.scalar(
            select(OutboxEvent).where(
                OutboxEvent.payload["payment_id"].astext == payment_id,
                OutboxEvent.topic == NEW_QUEUE,
            )
        )
        assert original_event is not None
        body = json.dumps(original_event.payload).encode()

    exchange = await rabbit_channel.get_exchange(EXCHANGE.name)
    await exchange.publish(aio_pika.Message(body), routing_key=NEW_QUEUE)

    # Контрольный платёж идёт следом в той же очереди: его обработка подтверждает обработку дубля.
    next_webhook_id = await create_webhook(client)
    next_payment = await submit_payment(client, next_webhook_id)
    await wait_for_webhook(client, next_payment["payment_id"])

    assert await get_webhook_requests(client, webhook_id) == requests_before
    payment_after = await get_payment(client, payment_id)
    assert payment_after["processed_at"] == payment_before["processed_at"]
    assert payment_after["status"] == payment_before["status"]


async def test_webhook_retries_after_one_and_two_seconds(client: httpx.AsyncClient) -> None:
    webhook_id = await create_webhook(client, statuses=[503, 503, 204])
    accepted = await submit_payment(client, webhook_id)
    await wait_for_webhook(client, accepted["payment_id"])

    attempts = await get_webhook_requests(client, webhook_id)
    assert len(attempts) == 3
    assert attempts[1]["time"] - attempts[0]["time"] >= 0.9
    assert attempts[2]["time"] - attempts[1]["time"] >= 1.9
    for attempt in attempts:
        assert attempt["payload"] == attempts[0]["payload"]
        assert attempt["event_id"] == attempt["idempotency_key"]


async def test_third_webhook_failure_goes_to_dead_queue(
    client: httpx.AsyncClient, dead_queue: AbstractQueue
) -> None:
    webhook_id = await create_webhook(client, statuses=[503])
    accepted = await submit_payment(client, webhook_id)

    message = await wait_for_dead_message(dead_queue)
    failed_event = json.loads(message.body)
    assert failed_event["payment_id"] == accepted["payment_id"]
    assert failed_event["attempt"] == 3
    assert failed_event["error_type"] == "HTTPStatusError"

    attempts = await get_webhook_requests(client, webhook_id)
    assert len(attempts) == 3
    payment = await get_payment(client, accepted["payment_id"])
    assert payment["status"] in ("succeeded", "failed")
    assert payment["processed_at"] is not None
    assert payment["webhook_delivered_at"] is None


async def test_invalid_event_goes_to_dead_queue(
    rabbit_channel: AbstractChannel, dead_queue: AbstractQueue
) -> None:
    exchange = await rabbit_channel.get_exchange(EXCHANGE.name)
    body = b"invalid-payment-json"
    await exchange.publish(aio_pika.Message(body), routing_key=NEW_QUEUE)

    message = await wait_for_dead_message(dead_queue)
    assert message.body == body
    assert message.headers["x-first-death-reason"] == "rejected"


async def test_outbox_failure_rolls_back_payment(engine: AsyncEngine) -> None:
    idempotency_key = uuid4().hex
    outbox_ids = []

    def break_outbox_insert(session, flush_context, instances):
        for obj in session.new:
            if isinstance(obj, OutboxEvent):
                outbox_ids.append(obj.id)
                # NULL нарушает ограничение БД и заставляет транзакцию откатиться.
                obj.topic = None

    async with AsyncSession(engine, expire_on_commit=False) as session:
        event.listen(session.sync_session, "before_flush", break_outbox_insert)
        with pytest.raises(IntegrityError):
            await create_payment(
                session, PaymentCreate(**payment_data(idempotency_key)), idempotency_key
            )

    async with AsyncSession(engine) as session:
        count = await session.scalar(
            select(func.count())
            .select_from(Payment)
            .where(Payment.idempotency_key == idempotency_key)
        )
        assert count == 0
        assert len(outbox_ids) == 1
        assert await session.get(OutboxEvent, outbox_ids[0]) is None


async def test_publication_survives_failure_before_outbox_commit(
    client: httpx.AsyncClient, engine: AsyncEngine
) -> None:
    webhook_id = await create_webhook(client)
    payment_id, event_id = uuid4(), uuid4()
    data = PaymentCreate(**payment_data(webhook_id))

    async with engine.begin() as connection:
        await connection.execute(
            text(f"""
            CREATE FUNCTION test_fail_outbox_commit() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN
                IF NEW.id = '{event_id}'::uuid AND NEW.published_at IS NOT NULL THEN
                    RAISE EXCEPTION 'simulated outbox commit failure';
                END IF;
                RETURN NEW;
            END $$
        """)
        )
        await connection.execute(
            text("""
            CREATE TRIGGER test_fail_outbox_commit BEFORE UPDATE ON outbox
            FOR EACH ROW EXECUTE FUNCTION test_fail_outbox_commit()
        """)
        )
    try:
        async with AsyncSession(engine) as session, session.begin():
            session.add(
                Payment(
                    id=payment_id,
                    amount=data.amount,
                    currency=data.currency,
                    description=data.description,
                    metadata_=data.metadata,
                    idempotency_key=webhook_id,
                    webhook_url=str(data.webhook_url),
                )
            )
            session.add(
                OutboxEvent(
                    id=event_id,
                    topic=NEW_QUEUE,
                    payload={
                        "event_id": str(event_id),
                        "payment_id": str(payment_id),
                        "attempt": 1,
                    },
                )
            )
        payment_before = await wait_for_webhook(client, str(payment_id))
        async with AsyncSession(engine) as session:
            stored_event = await session.get(OutboxEvent, event_id)
            assert stored_event.published_at is None
    finally:
        async with engine.begin() as connection:
            await connection.execute(text("DROP TRIGGER test_fail_outbox_commit ON outbox"))
            await connection.execute(text("DROP FUNCTION test_fail_outbox_commit()"))

    async def publication_committed() -> bool:
        async with AsyncSession(engine) as session:
            stored_event = await session.get(OutboxEvent, event_id)
            return stored_event.published_at is not None

    await wait_until(publication_committed)
    next_webhook_id = await create_webhook(client)
    next_payment = await submit_payment(client, next_webhook_id)
    await wait_for_webhook(client, next_payment["payment_id"])
    assert len(await get_webhook_requests(client, webhook_id)) == 1
    payment_after = await get_payment(client, str(payment_id))
    assert payment_after["processed_at"] == payment_before["processed_at"]
    assert payment_after["status"] == payment_before["status"]
