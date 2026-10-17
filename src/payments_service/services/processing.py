import logging
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from uuid import UUID

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payments_service.db.models import OutboxEvent, Payment, PaymentStatus
from payments_service.gateway import emulate_payment
from payments_service.messaging import retry_outbox_event
from payments_service.schemas import PaymentEvent, WebhookPayload

logger = logging.getLogger(__name__)


class PaymentProcessor:
    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        client: httpx.AsyncClient,
        gateway: Callable[[UUID], Awaitable[PaymentStatus]] = emulate_payment,
    ) -> None:
        self.sessions = sessions
        self.client = client
        self.gateway = gateway

    async def lock_payment(self, session: AsyncSession, event: PaymentEvent) -> Payment:
        payment = await session.scalar(
            select(Payment).where(Payment.id == event.payment_id).with_for_update()
        )
        if payment is None:
            raise LookupError("Payment not found")
        return payment

    async def completed(self, session: AsyncSession, payment: Payment, event: PaymentEvent) -> bool:
        return payment.webhook_delivered_at is not None or (
            await session.get(OutboxEvent, event.outcome_id) is not None
        )

    async def process(self, event: PaymentEvent) -> None:
        async with self.sessions.begin() as session:
            payment = await self.lock_payment(session, event)
            if await self.completed(session, payment, event):
                return
            if payment.status == PaymentStatus.PENDING:
                payment.status = await self.gateway(payment.id)
                payment.processed_at = datetime.now(UTC)

        async with self.sessions.begin() as session:
            payment = await self.lock_payment(session, event)
            if await self.completed(session, payment, event):
                return
            payload = WebhookPayload(
                event_id=event.event_id,
                payment_id=payment.id,
                amount=payment.amount,
                currency=payment.currency,
                status=payment.status,
                created_at=payment.created_at,
                processed_at=payment.processed_at,
            )
            try:
                response = await self.client.post(
                    payment.webhook_url,
                    json=payload.model_dump(mode="json"),
                    headers={
                        "X-Webhook-Id": str(event.event_id),
                        "Idempotency-Key": str(event.event_id),
                    },
                )
                response.raise_for_status()
            except httpx.HTTPError as exc:
                session.add(retry_outbox_event(event, type(exc).__name__))
                logger.warning(
                    "Webhook for payment %s failed on attempt %s: %s",
                    payment.id,
                    event.attempt,
                    type(exc).__name__,
                )
            else:
                payment.webhook_delivered_at = datetime.now(UTC)
