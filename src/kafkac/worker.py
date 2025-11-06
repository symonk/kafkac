from .handler import SingleMessageHandlerFunc
from .models import GroupedMessages


async def processor(messages: GroupedMessages, handler: SingleMessageHandlerFunc) -> None:
    """processor is responsible for processing messages received by the consumer
    for individual partitions."""
