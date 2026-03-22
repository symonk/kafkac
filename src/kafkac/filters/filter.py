import typing

from confluent_kafka import Message

# FilterFunc defines the signature for user defined filter functions
FilterFunc = typing.Callable[[Message], typing.Awaitable[bool]]


async def discard_message(topic: str, message: Message, filters: list[FilterFunc]) -> bool:
    """discard_message returns True if the message should be discarded by a
    filter for messages on a specific topic."""
    message_topic = message.topic()
    if message_topic != topic:
        return False
    for f in filters:
        if await f(message):
            return True
    return False


def filter_contains_header_fn(name: str) -> FilterFunc:
    """Only includes messages for processing within the fetched batch if
    the message contains an explicit header value (key).  The header value
    is irrelevant for the scope of this func.

    :returns: A boolean indicating the message should be processed by
    the consumer.
    """

    async def strategy(message: Message) -> bool:
        if (headers := message.headers()) is not None:
            for header, _ in headers:
                if header == name:
                    return True
        return False

    return strategy
