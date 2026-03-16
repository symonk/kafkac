from __future__ import annotations
import asyncio
from enum import StrEnum
from enum import auto
from collections.abc import Awaitable, Callable

from mergedeep import Strategy

_STRATEGY: dict[ProcessingOpt, Callable[..., Awaitable[set[asyncio.Task]]]]  = {}


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
async def by_topic() -> set[asyncio.Task]:
    return set()


@register(ProcessingOpt.BY_PARTITION)
async def by_partition() -> set[asyncio.Task]:
    return set()


@register(ProcessingOpt.MERGED)
async def merged() -> set[asyncio.Task]:
    return set()

@register(ProcessingOpt.BY_MESSAGE)
async def by_message() -> set[asyncio.Task]:
    return set()