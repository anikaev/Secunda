from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Header, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from payments_service.db.models import Payment
from payments_service.db.session import get_session
from payments_service.schemas import PaymentAccepted, PaymentCreate, PaymentDetail
from payments_service.services.payments import IdempotencyConflict, create_payment

router = APIRouter(prefix="/api/v1/payments", tags=["payments"])
Session = Annotated[AsyncSession, Depends(get_session)]


@router.post("", status_code=status.HTTP_202_ACCEPTED, response_model=PaymentAccepted)
async def post_payment(
    data: PaymentCreate,
    session: Session,
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key", min_length=1, max_length=255)],
) -> PaymentAccepted:
    if not idempotency_key.strip():
        raise HTTPException(status_code=422, detail="Idempotency-Key must not be blank")
    try:
        payment = await create_payment(session, data, idempotency_key)
    except IdempotencyConflict as exc:
        raise HTTPException(
            status_code=409, detail="Idempotency-Key was used for another request"
        ) from exc
    return PaymentAccepted.model_validate(payment)


@router.get("/{payment_id}", response_model=PaymentDetail)
async def get_payment(payment_id: UUID, session: Session) -> PaymentDetail:
    payment = await session.get(Payment, payment_id)
    if payment is None:
        raise HTTPException(status_code=404, detail="Payment not found")
    return PaymentDetail.model_validate(payment)
