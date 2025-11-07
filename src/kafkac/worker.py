from confluent_kafka import Message

from .handler import MessageHandlerFunc
from .handler import MessagesHandlerFunc
from .result import PartitionResult


async def worker(
    messages: list[Message], handler: MessageHandlerFunc | MessagesHandlerFunc
) -> PartitionResult:
    """processor is responsible for processing messages received by the consumer
    for individual partitions."""
    return PartitionResult(succeeded=messages)
