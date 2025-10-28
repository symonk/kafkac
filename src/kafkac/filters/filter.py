import typing

from confluent_kafka import Message

# FilterFunc defines the signature for user defined filter functions
FilterFunc = typing.Callable[[Message], typing.Awaitable[bool]]


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
