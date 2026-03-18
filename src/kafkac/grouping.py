from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable
from enum import StrEnum
from enum import auto

from confluent_kafka import Message
from mergedeep import Strategy

# TODO: Is dict right here, list[list[Message]] is probably enough?
_STRATEGY: dict[ProcessingOpt, Callable[..., dict[str, list[Message]]]]  = {}


def register(name: ProcessingOpt):
    def wrapper(fn: Strategy) -> Strategy:
        if name in _STRATEGY:
            raise ValueError(f"strategy already registered for: {name}")
        _STRATEGY[name] = fn
        return fn
    return wrapper



class ProcessingOpt(StrEnum):
    """ProcessingOpt encapsulates the options for a particular consumer in and governs
    what the user supplied batch handler will be invoked with.  These options are directly
    tied to a strategy function that is invoked by the consumer prior to the batch handling
    calls.  This governs the level of asyncio tasks that are generated"""
    BY_TOPIC = auto()
    BY_PARTITION = auto()
    MERGED = auto()
    BY_MESSAGE = auto()


@register(ProcessingOpt.BY_TOPIC)
def by_topic(messages: list[Message]) -> dict[str, list[Message]]:
    result = defaultdict(list)
    for message in messages:
        result[message.topic()].append(message)
    return result


@register(ProcessingOpt.BY_PARTITION)
def by_partition(messages: list[Message]) -> dict[str, list[Message]]:
    # TODO: Implement properly
    return {}


@register(ProcessingOpt.MERGED)
def merged(messages: list[Message]) -> dict[str, list[Message]]:
    # TODO: Implement properly
    return {}

@register(ProcessingOpt.BY_MESSAGE)
def by_message(messages: list[Message]) -> dict[str, list[Message]]:
    # TODO: Implement properly
    return {}