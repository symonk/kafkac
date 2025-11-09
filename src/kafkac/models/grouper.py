from __future__ import annotations

import typing
from collections import defaultdict

from confluent_kafka import Message


class MessageGrouper:
    """MessageGrouper encapsulates grouped messages across potentially multiple different
    topics, where each GroupedMessages within it, is a single partitions messages
    after the (optional) header level filtering has occurred."""

    def __init__(self) -> None:
        self.result: dict[str, typing.DefaultDict[int, list[Message]]] = {}

    def store(self, message: Message) -> None:
        """store appends the message to the appropriate GroupedMessages bucket.
        store creates new GroupedMessages buckets as necessary."""
        assert message.topic() is not None, "topic should never be none"
        assert message.partition() is not None, "partition should never be none"
        topic, partition = message.topic(), message.partition()
        if topic not in self.result:
            self.result[topic] = defaultdict(list)
        self.result[topic][partition].append(message)

    def __bool__(self) -> bool:
        return bool(self.result)
