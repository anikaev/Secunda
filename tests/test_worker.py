import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest
from faststream.rabbit import RabbitBroker, RabbitMessage

from payments_service.schemas import PaymentEvent
from payments_service.services.processing import PaymentProcessor
from payments_service.worker import PaymentWorker


@pytest.fixture
def broker() -> MagicMock:
    broker = MagicMock(spec=RabbitBroker)
    broker.publish.return_value = True
    return broker


@pytest.fixture
def message() -> MagicMock:
    return MagicMock(spec=RabbitMessage)


async def test_worker_acknowledges_success(
    event: PaymentEvent, message: MagicMock, broker: MagicMock
) -> None:
    processor = MagicMock(spec=PaymentProcessor)
    await PaymentWorker(processor, broker, 5).handle(event.model_dump_json().encode(), message)
    processor.process.assert_awaited_once_with(event)
    message.ack.assert_awaited_once()
    broker.publish.assert_not_awaited()


@pytest.mark.parametrize("body", [b"not-json", b"{}", b'{"attempt":4}', b"null"])
async def test_worker_rejects_malformed_message(
    body: bytes, message: MagicMock, broker: MagicMock
) -> None:
    processor = MagicMock(spec=PaymentProcessor)
    await PaymentWorker(processor, broker, 5).handle(body, message)
    message.reject.assert_awaited_once_with(requeue=False)
    message.ack.assert_not_awaited()
    processor.process.assert_not_awaited()


@pytest.mark.parametrize(("attempt", "topic"), [(1, "payments.retry.2"), (3, "payments.dead")])
async def test_worker_publishes_retry_before_ack(
    event: PaymentEvent, message: MagicMock, broker: MagicMock, attempt: int, topic: str
) -> None:
    processor = MagicMock(spec=PaymentProcessor)
    processor.process.side_effect = OSError("database unavailable")

    async def publish(*args, **kwargs):
        message.ack.assert_not_awaited()
        return True

    broker.publish.side_effect = publish
    event = event.model_copy(update={"attempt": attempt})
    await PaymentWorker(processor, broker, 5).handle(event.model_dump_json().encode(), message)
    assert broker.publish.call_args.kwargs["routing_key"] == topic
    assert broker.publish.call_args.kwargs["persist"] is True
    assert broker.publish.call_args.kwargs["mandatory"] is True
    message.ack.assert_awaited_once()


async def test_worker_keeps_message_when_retry_cannot_be_published(
    event: PaymentEvent, message: MagicMock, broker: MagicMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    processor = MagicMock(spec=PaymentProcessor)
    processor.process.side_effect = OSError("database unavailable")
    broker.publish.side_effect = ConnectionError("broker unavailable")
    monkeypatch.setattr("payments_service.worker.asyncio.sleep", AsyncMock())
    await PaymentWorker(processor, broker, 5).handle(event.model_dump_json().encode(), message)
    message.ack.assert_not_awaited()
    message.nack.assert_awaited_once_with(requeue=True)


async def test_cancellation_does_not_acknowledge_message(
    event: PaymentEvent, message: MagicMock, broker: MagicMock
) -> None:
    processor = MagicMock(spec=PaymentProcessor)
    processor.process.side_effect = asyncio.CancelledError
    with pytest.raises(asyncio.CancelledError):
        await PaymentWorker(processor, broker, 5).handle(event.model_dump_json().encode(), message)
    message.ack.assert_not_awaited()
    broker.publish.assert_not_awaited()
