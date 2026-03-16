import asyncio
from enum import StrEnum
from enum import auto

_STRATEGY = {}


def register(name: str):
    def wrapper(cls):
        _STRATEGY[name] = cls
        return cls
    return wrapper



class ProcessingOpt(StrEnum):
    """ProcessingOpt encapsulates the options for a particular consumer in and governs
    what the user supplied batch handler will be invoked with.  These options are directly
    tied to a strategy function that is invoked by the consumer prior to the batch handling
    calls.  This governs the level of asyncio tasks that are generated"""
    ByTopic = auto()
    ByPartition = auto()
    Merged = auto()
    ByMessage = auto()


@register(ProcessingOpt.ByTopic)
async def by_topic() -> set[asyncio.Task]:
    return set()


@register(ProcessingOpt.ByPartition)
async def by_partition() -> set[asyncio.Task]:
    return set()


@register(ProcessingOpt.Merged)
async def merged() -> set[asyncio.Task]:
    return set()

@register(ProcessingOpt.ByMessage)
async def by_message() -> set[asyncio.Task]:
    return set()