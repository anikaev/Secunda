from faststream.rabbit import QueueType, RabbitBroker, RabbitExchange, RabbitQueue

from payments_service.db.models import OutboxEvent
from payments_service.schemas import PaymentEvent

NEW_QUEUE = "payments.new"
DEAD_QUEUE = "payments.dead"
EXCHANGE = RabbitExchange("payments", durable=True)
DEAD_EXCHANGE = RabbitExchange("payments.dlx", durable=True)


def retry_outbox_event(event: PaymentEvent, error_type: str) -> OutboxEvent:
    if event.attempt == 3:
        next_attempt = event.attempt
        topic = DEAD_QUEUE
    else:
        next_attempt = event.attempt + 1
        topic = f"payments.retry.{next_attempt}"

    retry = PaymentEvent(
        event_id=event.event_id,
        payment_id=event.payment_id,
        attempt=next_attempt,
        error_type=error_type,
    )
    return OutboxEvent(
        id=event.retry_id,
        topic=topic,
        payload=retry.model_dump(mode="json", exclude_none=True),
    )


class RabbitTopology:
    def __init__(self) -> None:
        self.main_queue = self.dead_letter_queue(NEW_QUEUE, DEAD_EXCHANGE.name, DEAD_QUEUE)
        self.dead_queue = RabbitQueue(DEAD_QUEUE, queue_type=QueueType.QUORUM, durable=True)
        self.retry_queues = [
            self.dead_letter_queue(f"payments.retry.{attempt}", EXCHANGE.name, NEW_QUEUE, ttl_ms)
            for attempt, ttl_ms in ((2, 1000), (3, 2000))
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
