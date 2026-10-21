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
        webhook_client: httpx.AsyncClient,
        gateway: Callable[[UUID], Awaitable[PaymentStatus]] = emulate_payment,
    ) -> None:
        self.sessions = sessions
        self.webhook_client = webhook_client
        self.gateway = gateway

    async def process(self, event: PaymentEvent) -> None:
        # Сохраняем оплату отдельно: ошибка webhook не должна запускать шлюз заново.
        async with self.sessions.begin() as session:
            payment = await self._lock_payment(session, event.payment_id)
            if await self._already_handled(session, payment, event):
                return
            if payment.status == PaymentStatus.PENDING:
                payment.status = await self.gateway(payment.id)
                payment.processed_at = datetime.now(UTC)

        async with self.sessions.begin() as session:
            payment = await self._lock_payment(session, event.payment_id)
            if await self._already_handled(session, payment, event):
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
                response = await self.webhook_client.post(
                    payment.webhook_url,
                    json=payload.model_dump(mode="json"),
                    headers={
                        "X-Webhook-Id": str(event.event_id),
                        "Idempotency-Key": str(event.event_id),
                    },
                )
                response.raise_for_status()
            except httpx.HTTPError as exc:
                error_type = type(exc).__name__
                session.add(retry_outbox_event(event, error_type))
                logger.warning(
                    "Webhook for payment %s failed on attempt %s: %s",
                    payment.id,
                    event.attempt,
                    error_type,
                )
            else:
                payment.webhook_delivered_at = datetime.now(UTC)

    async def _lock_payment(self, session: AsyncSession, payment_id: UUID) -> Payment:
        payment = await session.scalar(
            select(Payment).where(Payment.id == payment_id).with_for_update()
        )
        if payment is None:
            raise LookupError("Payment not found")
        return payment

    async def _already_handled(
        self, session: AsyncSession, payment: Payment, event: PaymentEvent
    ) -> bool:
        if payment.webhook_delivered_at is not None:
            return True
        retry = await session.get(OutboxEvent, event.retry_id)
        return retry is not None
