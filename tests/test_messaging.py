from pamqp.decode import field_table as decode_table
from pamqp.encode import field_table as encode_table

from payments_service.messaging import RabbitTopology


def test_queue_arguments_can_be_encoded_by_locked_amqp_client() -> None:
    topology = RabbitTopology()
    for queue in [topology.main_queue, topology.dead_queue, *topology.retry_queues]:
        arguments = queue.arguments
        _, decoded = decode_table(encode_table(arguments))
        assert decoded == arguments
