from dataclasses import dataclass
from enum import StrEnum


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
