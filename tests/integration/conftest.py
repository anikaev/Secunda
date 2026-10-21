import os
from collections.abc import AsyncIterator

import aio_pika
import httpx
import pytest
from aio_pika.abc import AbstractChannel, AbstractQueue
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine


@pytest.fixture
async def client() -> AsyncIterator[httpx.AsyncClient]:
    async with httpx.AsyncClient(timeout=10, trust_env=False) as client:
        yield client


@pytest.fixture
async def engine() -> AsyncIterator[AsyncEngine]:
    engine = create_async_engine(os.environ["DATABASE_URL"])
    try:
        yield engine
    finally:
        await engine.dispose()


@pytest.fixture
async def rabbit_channel() -> AsyncIterator[AbstractChannel]:
    connection = await aio_pika.connect_robust(os.environ["RABBITMQ_URL"])
    async with connection:
        yield await connection.channel(publisher_confirms=True)


@pytest.fixture
async def dead_queue(rabbit_channel: AbstractChannel) -> AbstractQueue:
    return await rabbit_channel.get_queue("payments.dead")
