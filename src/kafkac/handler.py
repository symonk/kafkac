import typing

from confluent_kafka import Message

from .result import PartitionResult


@typing.runtime_checkable
class MessageHandlerFunc(typing.Protocol):
    """MessageHandlerFunc handles a single message."""

    async def __call__(self, message: Message) -> PartitionResult: ...


@typing.runtime_checkable
class MessagesHandlerFunc(typing.Protocol):
    """MessagesHandlerFunc handles multiple messages."""

    async def __call__(self, messages: list[Message]) -> PartitionResult: ...
