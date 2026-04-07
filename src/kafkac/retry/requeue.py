import typing

from confluent_kafka import Message


class Forwarder(typing.Protocol):
    async def forward(self, message: Message) -> None: ...

    async def forward_many(self, message: list[Message]) -> None: ...
