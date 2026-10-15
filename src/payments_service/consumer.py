import logging

from faststream import FastStream
from faststream.rabbit import RabbitBroker

from payments_service.core.config import get_settings

settings = get_settings()
logging.basicConfig(level=settings.log_level)

broker = RabbitBroker(settings.rabbitmq_url.get_secret_value())
app = FastStream(broker)
