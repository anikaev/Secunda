import asyncio
import random
from uuid import UUID

from payments_service.db.models import PaymentStatus


async def emulate_payment(payment_id: UUID) -> PaymentStatus:
    await asyncio.sleep(random.uniform(2, 5))
    return PaymentStatus.SUCCEEDED if random.random() < 0.9 else PaymentStatus.FAILED
