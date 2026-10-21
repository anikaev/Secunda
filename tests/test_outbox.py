from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest
from faststream.rabbit import RabbitBroker

from payments_service.config import Settings
from payments_service.db.models import OutboxEvent
from payments_service.outbox import OutboxRelay
from payments_service.schemas import PaymentEvent


@pytest.fixture
def outbox_event(event: PaymentEvent) -> OutboxEvent:
    return OutboxEvent(
        id=event.event_id,
        topic="payments.new",
        payload=event.model_dump(mode="json"),
        attempts=0,
        available_at=datetime.now(UTC),
        published_at=None,
    )


@pytest.mark.parametrize("confirmation", [True, None])
async def test_outbox_requires_publication_confirmation(
    outbox_event: OutboxEvent,
    session: MagicMock,
    sessions: MagicMock,
    settings: Settings,
    confirmation: object,
) -> None:
    session.scalar.return_value = outbox_event
    broker = MagicMock(spec=RabbitBroker)
    broker.publish.return_value = confirmation
    await OutboxRelay(sessions, broker, settings).publish_one()
    assert (outbox_event.published_at is not None) == bool(confirmation)
    assert outbox_event.attempts == 1


async def test_outbox_keeps_event_and_backs_off_on_failure(
    outbox_event: OutboxEvent, session: MagicMock, sessions: MagicMock, settings: Settings
) -> None:
    session.scalar.return_value = outbox_event
    broker = MagicMock(spec=RabbitBroker)
    broker.publish.side_effect = ConnectionError("broker unavailable")
    relay = OutboxRelay(sessions, broker, settings)
    before = datetime.now(UTC)
    await relay.publish_one()
    first_delay = outbox_event.available_at - before
    before = datetime.now(UTC)
    await relay.publish_one()
    second_delay = outbox_event.available_at - before
    assert first_delay.total_seconds() >= 1
    assert second_delay.total_seconds() >= 2
    assert outbox_event.published_at is None
    assert outbox_event.last_error == "ConnectionError"
