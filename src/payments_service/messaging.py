from faststream.rabbit import QueueType, RabbitBroker, RabbitExchange, RabbitQueue

from payments_service.db.models import OutboxEvent
from payments_service.schemas import PaymentEvent

NEW_QUEUE = "payments.new"
DEAD_QUEUE = "payments.dead"
EXCHANGE = RabbitExchange("payments", durable=True)
DEAD_EXCHANGE = RabbitExchange("payments.dlx", durable=True)


def retry_outbox_event(event: PaymentEvent, error_type: str) -> OutboxEvent:
    final_attempt = event.attempt == 3
    next_event = event.model_copy(
        update={
            "attempt": event.attempt if final_attempt else event.attempt + 1,
            "error_type": error_type,
        }
    )
    return OutboxEvent(
        id=event.outcome_id,
        topic=DEAD_QUEUE if final_attempt else f"payments.retry.{next_event.attempt}",
        payload=next_event.model_dump(mode="json", exclude_none=True),
    )


class RabbitTopology:
    def __init__(self) -> None:
        self.main_queue = self.dead_letter_queue(NEW_QUEUE, DEAD_EXCHANGE.name, DEAD_QUEUE)
        self.dead_queue = RabbitQueue(DEAD_QUEUE, queue_type=QueueType.QUORUM, durable=True)
        self.retry_queues = [
            self.dead_letter_queue(
                f"payments.retry.{attempt}",
                EXCHANGE.name,
                NEW_QUEUE,
                ttl_ms=1000 * 2 ** (attempt - 2),
            )
            for attempt in (2, 3)
        ]

    @staticmethod
    def dead_letter_queue(
        name: str, exchange: str, routing_key: str, ttl_ms: int | None = None
    ) -> RabbitQueue:
        arguments = {
            "x-dead-letter-exchange": exchange,
            "x-dead-letter-routing-key": routing_key,
            "x-dead-letter-strategy": "at-least-once",
            "x-overflow": "reject-publish",
            "x-delivery-limit": -1,
        }
        if ttl_ms is not None:
            arguments["x-message-ttl"] = ttl_ms
        return RabbitQueue(
            name,
            queue_type=QueueType.QUORUM,
            durable=True,
            arguments=arguments,
        )

    async def declare(self, broker: RabbitBroker) -> None:
        exchange = await broker.declare_exchange(EXCHANGE)
        dead_exchange = await broker.declare_exchange(DEAD_EXCHANGE)
        dead_queue = await broker.declare_queue(self.dead_queue)
        await dead_queue.bind(dead_exchange, routing_key=DEAD_QUEUE)
        for definition in [self.main_queue, *self.retry_queues]:
            queue = await broker.declare_queue(definition)
            await queue.bind(exchange, routing_key=definition.name)


async def publish_event(broker: RabbitBroker, event: OutboxEvent, publish_timeout: float) -> None:
    confirmation = await broker.publish(
        event.payload,
        exchange=DEAD_EXCHANGE if event.topic == DEAD_QUEUE else EXCHANGE,
        routing_key=event.topic,
        message_id=str(event.id),
        persist=True,
        mandatory=True,
        timeout=publish_timeout,
    )
    if not confirmation:
        raise RuntimeError("Broker did not confirm publication")
