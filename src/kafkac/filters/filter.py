import typing
from dataclasses import dataclass

from confluent_kafka import Message

# FilterFunc defines the signature for user defined filter functions
FilterFunc = typing.Callable[[Message], typing.Awaitable[bool]]


@dataclass(frozen=True)
class FilterFuncs:
    """FilterFuncs is a mechanism to inspect the message prior to including it in batching.
    This allows messages to be skipped very early without the overhead of deserialisation
    in cases where a consumer should not concern itself with the message.

    The `FilterFunc` objects contract stipulates, that if a message should be included by
    the consumer for processing, it should return `False` as part of filtering.  Filtering
    as a concept is around signalling to the consumer that a message should be ignored.
    """
    topics: set[str]
    funcs: list[FilterFunc]

    def __post_init__(self) -> None:
        if len(self.funcs) == 0:
            raise ValueError("cannot use FilterFuncs without filter functions")

    def _applicable(self, topic: str) -> bool:
        """applicable returns in O(1) if the topic of the message is
        applicable to the topics registered in this instance."""
        return len(self.topics) > 0 and topic in self.topics

    async def should_discard(self, message: Message) -> bool:
        """should_discard iterates the registered functions for the provided topics
        and if the message should be discarded, signals the consumer to move forward
        ignoring any processing for that particular message."""
        if not self._applicable(message.topic()):
            return False
        for func in self.funcs: # FIFO
            if await func(message):
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
