from confluent_kafka import Message

from .handler import MessagesHandlerFunc
from .result import PartitionResult


async def batch_worker(
    messages: list[Message], handler: MessagesHandlerFunc
) -> PartitionResult:
    """processor is responsible for processing messages received by the consumer
    for individual partitions."""
    return await handler(messages)
