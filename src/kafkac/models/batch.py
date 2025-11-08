from __future__ import annotations

import typing
from collections import defaultdict
from dataclasses import dataclass
from dataclasses import field

from confluent_kafka import Message


# TODO: This concept needs alot of work, very buggy etc, is sticking all messages and
# not partitioning properly!
@dataclass
class Batch:
    """Batch encapsulates grouped messages across potentially multiple different
    topics, where each GroupedMessages within it, is a single partitions messages
    after the (optional) header level filtering has occurred."""

    result: typing.DefaultDict[str, list[Message]] = field(
        default_factory=lambda: defaultdict(list)
    )

    def store(self, message: Message) -> None:
        """store appends the message to the appropriate GroupedMessages bucket.
        store creates new GroupedMessages buckets as necessary."""
        assert message.topic() is not None, "topic should never be none"
        self.result[message.topic()].append(message)

    def __bool__(self) -> bool:
        return bool(self.result)
