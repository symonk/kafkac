from .handler import HandlerFunc
from .models import GroupedMessages


async def processor(messages: GroupedMessages, handler: HandlerFunc) -> None:
    """processor is responsible for processing messages received by the consumer
    for individual partitions."""
