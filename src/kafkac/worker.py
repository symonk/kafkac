from .handler import MessageHandlerFunc
from .models import GroupedMessages


async def processor(messages: GroupedMessages, handler: MessageHandlerFunc) -> None:
    """processor is responsible for processing messages received by the consumer
    for individual partitions."""
