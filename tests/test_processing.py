import json
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest

from payments_service.db.models import Payment, PaymentStatus
from payments_service.schemas import PaymentEvent
from payments_service.services.processing import PaymentProcessor


async def test_webhook_retry_does_not_charge_payment_twice(
    payment: Payment, event: PaymentEvent, session: MagicMock, sessions: MagicMock
) -> None:
    requests = []

    def webhook(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(500 if len(requests) == 1 else 204)

    gateway = AsyncMock(return_value=PaymentStatus.SUCCEEDED)
    async with httpx.AsyncClient(transport=httpx.MockTransport(webhook)) as client:
        processor = PaymentProcessor(sessions, client, gateway)
        await processor.process(event)
        assert payment.status == PaymentStatus.SUCCEEDED
        assert payment.webhook_delivered_at is None
        retry = session.add.call_args.args[0]
        assert retry.topic == "payments.retry.2"
        await processor.process(PaymentEvent.model_validate(retry.payload))
        await processor.process(event)

    gateway.assert_awaited_once_with(payment.id)
    assert len(requests) == 2
    assert requests[0].content == requests[1].content
    assert requests[0].headers["Idempotency-Key"] == str(event.event_id)
    assert payment.webhook_delivered_at is not None


async def test_third_webhook_failure_goes_to_dlq(
    payment: Payment, event: PaymentEvent, session: MagicMock, sessions: MagicMock
) -> None:
    gateway = AsyncMock(return_value=PaymentStatus.SUCCEEDED)
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(503))
    ) as client:
        processor = PaymentProcessor(sessions, client, gateway)
        for attempt in (1, 2, 3):
            await processor.process(event.model_copy(update={"attempt": attempt}))

    assert [call.args[0].topic for call in session.add.call_args_list] == [
        "payments.retry.2",
        "payments.retry.3",
        "payments.dead",
    ]
    assert session.add.call_args.args[0].payload["attempt"] == 3
    assert payment.status == PaymentStatus.SUCCEEDED
    assert payment.webhook_delivered_at is None
    gateway.assert_awaited_once()


async def test_scheduled_retry_prevents_repeating_current_attempt(
    event: PaymentEvent, session: MagicMock, sessions: MagicMock
) -> None:
    post = AsyncMock(
        return_value=httpx.Response(503, request=httpx.Request("POST", "https://test"))
    )
    client = MagicMock(spec=httpx.AsyncClient)
    client.post = post
    processor = PaymentProcessor(sessions, client, AsyncMock(return_value=PaymentStatus.SUCCEEDED))
    await processor.process(event)
    session.get.return_value = session.add.call_args.args[0]
    await processor.process(event)
    post.assert_awaited_once()


async def test_declined_payment_is_final_and_not_retried(
    payment: Payment, event: PaymentEvent, session: MagicMock, sessions: MagicMock
) -> None:
    requests = []

    def webhook(request: httpx.Request) -> httpx.Response:
        requests.append(json.loads(request.content))
        return httpx.Response(200)

    async with httpx.AsyncClient(transport=httpx.MockTransport(webhook)) as client:
        processor = PaymentProcessor(sessions, client, AsyncMock(return_value=PaymentStatus.FAILED))
        await processor.process(event)
    assert payment.status == PaymentStatus.FAILED
    assert requests[0]["status"] == "failed"
    assert payment.webhook_delivered_at is not None
    session.add.assert_not_called()


async def test_network_failure_schedules_retry(
    event: PaymentEvent, session: MagicMock, sessions: MagicMock
) -> None:
    def webhook(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection unavailable", request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(webhook)) as client:
        processor = PaymentProcessor(
            sessions, client, AsyncMock(return_value=PaymentStatus.SUCCEEDED)
        )
        await processor.process(event)
    assert session.add.call_args.args[0].topic == "payments.retry.2"


async def test_webhook_waits_for_payment_commit(event: PaymentEvent, sessions: MagicMock) -> None:
    sessions.begin.return_value.__aexit__.side_effect = RuntimeError("commit failed")
    client = MagicMock(spec=httpx.AsyncClient)
    processor = PaymentProcessor(sessions, client, AsyncMock(return_value=PaymentStatus.SUCCEEDED))
    with pytest.raises(RuntimeError, match="commit failed"):
        await processor.process(event)
    client.post.assert_not_awaited()
