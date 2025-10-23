import typing
from dataclasses import dataclass
from enum import StrEnum

from confluent_kafka import Message

# DLQFunc defines the coroutine signature for user supplied dead letter handling
DLQFunc = typing.Callable[[Message], typing.Awaitable[bool]]


class OnFail(StrEnum):
    """OnFail is an enumeration that dictates the behaviour of the
    (optional) dead letter queue algorithm."""

    LAX = "lax"
    STRICT = "strict"
    IGNORE = "ignore"


@dataclass(frozen=True)
class DeadLetterQueueConfig:
    MaxFails: int
    Topic: str
    OnFail: OnFail
