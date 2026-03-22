from confluent_kafka import Message

from .handler import MessagesHandlerFunc
from .result import HandlerResultContext


async def message_processor(
    messages: list[Message], handler: MessagesHandlerFunc
) -> HandlerResultContext:
    """processor is responsible for processing messages received by the consumer
    for individual partitions."""
    return await handler(messages)
