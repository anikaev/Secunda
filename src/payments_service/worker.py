import asyncio
import logging

from faststream.rabbit import RabbitBroker, RabbitMessage
from pydantic import ValidationError

from payments_service.messaging import publish_event, retry_outbox_event
from payments_service.schemas import PaymentEvent
from payments_service.services.processing import PaymentProcessor

logger = logging.getLogger(__name__)


class PaymentWorker:
    def __init__(
        self, processor: PaymentProcessor, broker: RabbitBroker, publish_timeout: float
    ) -> None:
        self.processor = processor
        self.broker = broker
        self.publish_timeout = publish_timeout

    async def handle(self, body: bytes, message: RabbitMessage) -> None:
        try:
            event = PaymentEvent.model_validate_json(body)
        except ValidationError:
            logger.warning("Invalid payment event rejected")
            await message.reject(requeue=False)
            return
        try:
            await self.processor.process(event)
        except Exception as exc:
            logger.error("Payment event %s failed: %s", event.event_id, type(exc).__name__)
            try:
                await publish_event(
                    self.broker, retry_outbox_event(event, type(exc).__name__), self.publish_timeout
                )
            except Exception as publish_error:
                logger.error("Retry publication failed: %s", type(publish_error).__name__)
                await asyncio.sleep(1)
                await message.nack(requeue=True)
                return
        await message.ack()
