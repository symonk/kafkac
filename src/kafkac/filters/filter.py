import typing

from confluent_kafka import Message

# FilterFunc defines the signature for user defined filter functions
FilterFunc = typing.Callable[[Message], typing.Awaitable[bool]]


def filter_contains_header_fn(name: str) -> FilterFunc:
    """filter_contains_header_fn is a built in"""

    async def strategy(message: Message) -> bool:
        if (headers := message.headers()) is not None:
            for header, _ in headers:
                if header == name:
                    return True
        return False

    return strategy
