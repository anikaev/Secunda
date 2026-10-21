import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress

import httpx
from faststream import AckPolicy, FastStream
from faststream.rabbit import Channel, RabbitBroker
from faststream.rabbit.annotations import RabbitMessage

from payments_service.config import Settings, get_settings
from payments_service.db.session import Database
from payments_service.messaging import EXCHANGE, RabbitTopology
from payments_service.outbox import OutboxRelay
from payments_service.services.processing import PaymentProcessor
from payments_service.worker import PaymentWorker


def create_consumer(settings: Settings) -> FastStream:
    logging.basicConfig(level=settings.log_level)
    database = Database(settings)
    webhook_client = httpx.AsyncClient(
        timeout=settings.webhook_timeout, follow_redirects=False, trust_env=False
    )
    broker = RabbitBroker(
        settings.rabbitmq_url.get_secret_value(),
        timeout=settings.publish_timeout,
        default_channel=Channel(prefetch_count=1, publisher_confirms=True, on_return_raises=True),
        graceful_timeout=20,
    )
    topology = RabbitTopology()
    processor = PaymentProcessor(database.sessions, webhook_client)
    worker = PaymentWorker(processor, broker, settings.publish_timeout)
    relay = OutboxRelay(database.sessions, broker, settings)
    stop = asyncio.Event()
    relay_task: asyncio.Task[None] | None = None

    @asynccontextmanager
    async def lifespan() -> AsyncIterator[None]:
        try:
            yield
        finally:
            stop.set()
            if relay_task is not None:
                relay_task.cancel()
                with suppress(asyncio.CancelledError):
                    await relay_task
            await broker.stop()
            await webhook_client.aclose()
            await database.close()

    app = FastStream(broker, lifespan=lifespan)

    @app.on_startup
    async def connect_broker() -> None:
        await broker.connect()
        await topology.declare(broker)

    @app.after_startup
    async def start_relay() -> None:
        nonlocal relay_task
        relay_task = asyncio.create_task(relay.run(stop))

    @app.on_shutdown
    async def stop_relay() -> None:
        stop.set()
        if relay_task is not None:
            with suppress(TimeoutError):
                await asyncio.wait_for(relay_task, timeout=settings.publish_timeout + 5)

    @broker.subscriber(
        topology.main_queue,
        EXCHANGE,
        ack_policy=AckPolicy.MANUAL,
        decoder=lambda message: message.body,
    )
    async def handle_payment(body: bytes, message: RabbitMessage) -> None:
        await worker.handle(body, message)

    return app


app = create_consumer(get_settings())
