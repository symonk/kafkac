import typing

from confluent_kafka import Message

from .result import KafkacContext


@typing.runtime_checkable
class MessagesHandlerFunc(typing.Protocol):
    """MessagesHandlerFunc handles multiple messages."""

    async def __call__(
        self, context: KafkacContext, messages: list[Message]
    ) -> KafkacContext: ...
