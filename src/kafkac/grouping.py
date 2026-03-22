from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable
from enum import StrEnum
from enum import auto

from confluent_kafka import Message

ParallelType = list[list[Message]]


class ProcessingOpt(StrEnum):
    """ProcessingOpt encapsulates the options for a particular consumer in and governs
    what the user supplied batch handler will be invoked with.  These options are directly
    tied to a strategy function that is invoked by the consumer prior to the batch handling
    calls.  This governs the level of asyncio tasks that are generated"""

    BY_TOPIC = auto()
    BY_PARTITION = auto()
    MERGED = auto()
    BY_MESSAGE = auto()


def by_topic(messages: list[Message]) -> ParallelType:
    """by_topic groups the messages polled from kafka by their topic initially
    and finally returns a list[list[Message]] where each element within the
    list is for a unique topic, but all partitions within that topic which
    are assigned to this consumer."""
    result = defaultdict(list)
    for message in messages:
        result[message.topic()].append(message)
    return [r for r in result.values()]


def by_partition(messages: list[Message]) -> ParallelType:
    """by_partition groups the messages polled from kafka by their (topic, partition)
    combinations."""
    result = defaultdict(list)
    for message in messages:
        t, p = message.topic(), message.partition()
        result[(t, p)].append(message)
    return [r for r in result.values()]


def merged(messages: list[Message]) -> ParallelType:
    """merged returns all messages from all topics and partitions, wrapping
    the input simply in a 1 element length list."""
    return [messages]


def by_message(messages: list[Message]) -> ParallelType:
    """by_message creates many lists of size 1, where each message is its own
    list."""
    return [[m] for m in messages]


# TODO: Is dict right here, list[list[Message]] is probably enough?
_STRATEGY: dict[ProcessingOpt, Callable[..., ParallelType]] = {
    ProcessingOpt.BY_TOPIC: by_topic,
    ProcessingOpt.BY_PARTITION: by_partition,
    ProcessingOpt.MERGED: merged,
    ProcessingOpt.BY_MESSAGE: by_message,
}
