from .result import PartitionResult
from .handler import MessageHandlerFunc
from .handler import MessagesHandlerFunc
from .models import GroupedMessages


async def worker(
    messages: GroupedMessages, handler: MessageHandlerFunc | MessagesHandlerFunc
) -> PartitionResult:
    """processor is responsible for processing messages received by the consumer
    for individual partitions."""
    return PartitionResult(messages)
