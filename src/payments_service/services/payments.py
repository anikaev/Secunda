from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from payments_service.db.models import OutboxEvent, Payment
from payments_service.messaging import NEW_QUEUE
from payments_service.schemas import PaymentCreate, PaymentEvent


class IdempotencyConflict(Exception):
    pass


async def create_payment(
    session: AsyncSession, data: PaymentCreate, idempotency_key: str
) -> Payment:
    async with session.begin():
        statement = (
            insert(Payment)
            .values(
                id=uuid4(),
                amount=data.amount,
                currency=data.currency,
                description=data.description,
                metadata_=data.metadata,
                webhook_url=str(data.webhook_url),
                idempotency_key=idempotency_key,
            )
            .on_conflict_do_nothing(index_elements=[Payment.idempotency_key])
            .returning(Payment)
        )
        payment = await session.scalar(statement)
        if payment is not None:
            event = PaymentEvent(event_id=uuid4(), payment_id=payment.id)
            session.add(
                OutboxEvent(
                    id=event.event_id,
                    topic=NEW_QUEUE,
                    payload=event.model_dump(mode="json", exclude_none=True),
                )
            )
            return payment

        payment = await session.scalar(
            select(Payment).where(Payment.idempotency_key == idempotency_key)
        )
        if payment is None:
            raise RuntimeError("Conflicting payment was not found")
        if (
            payment.amount != data.amount
            or payment.currency != data.currency
            or payment.description != data.description
            or payment.metadata_ != data.metadata
            or payment.webhook_url != str(data.webhook_url)
        ):
            raise IdempotencyConflict
        return payment
