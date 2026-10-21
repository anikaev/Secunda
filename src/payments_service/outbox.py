import asyncio
import logging
from contextlib import suppress
from datetime import UTC, datetime, timedelta

from faststream.rabbit import RabbitBroker
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payments_service.config import Settings
from payments_service.db.models import OutboxEvent
from payments_service.messaging import publish_event

logger = logging.getLogger(__name__)


class OutboxRelay:
    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        broker: RabbitBroker,
        settings: Settings,
    ) -> None:
        self.sessions = sessions
        self.broker = broker
        self.settings = settings

    async def publish_one(self) -> bool:
        async with self.sessions.begin() as session:
            event = await session.scalar(
                select(OutboxEvent)
                .where(OutboxEvent.published_at.is_(None), OutboxEvent.available_at <= func.now())
                .order_by(OutboxEvent.available_at, OutboxEvent.created_at)
                .with_for_update(skip_locked=True)
                .limit(1)
            )
            if event is None:
                return False
            event.attempts += 1
            try:
                await publish_event(self.broker, event, self.settings.publish_timeout)
            except Exception as exc:
                retry_delay = 2 ** min(event.attempts - 1, 20)
                delay = min(retry_delay, self.settings.outbox_retry_max_delay)
                event.available_at = datetime.now(UTC) + timedelta(seconds=delay)
                event.last_error = type(exc).__name__
                logger.warning("Outbox event %s was not published: %s", event.id, event.last_error)
            else:
                event.published_at = datetime.now(UTC)
                event.last_error = None
        return True

    async def run(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            try:
                for _ in range(self.settings.outbox_batch_size):
                    if stop.is_set() or not await self.publish_one():
                        break
            except Exception as exc:
                logger.error("Outbox relay failed: %s", type(exc).__name__)
            with suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=self.settings.outbox_poll_interval)
