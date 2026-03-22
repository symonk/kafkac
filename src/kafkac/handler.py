import typing

from confluent_kafka import Message

from .result import HandlerResultContext


@typing.runtime_checkable
class MessagesHandlerFunc(typing.Protocol):
    """MessagesHandlerFunc handles multiple messages."""

    async def __call__(
        self, context: HandlerResultContext, messages: list[Message]
    ) -> HandlerResultContext: ...
