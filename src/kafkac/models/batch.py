from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field

from confluent_kafka import Message


@dataclass
class Batch:
    """Batch encapsulates grouped messages across potentially multiple different
    topics, where each GroupedMessages within it, is a single partitions messages
    after the (optional) header level filtering has occurred."""

    messages: list[GroupedMessages] = field(default_factory=list)

    def store(self, message: Message) -> None:
        """store appends the message to the appropriate GroupedMessages bucket.
        store creates new GroupedMessages buckets as necessary."""

    def __bool__(self) -> bool:
        return bool(self.messages)


@dataclass
class GroupedMessages:
    """GroupedMessages encapsulates per partition messages for a given
    topic"""

    topic: str
    partition: int
    messages: list[Message] = field(default_factory=list)
