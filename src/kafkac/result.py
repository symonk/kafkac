from dataclasses import dataclass
from dataclasses import field

from confluent_kafka import Message


@dataclass
class PartitionResult:
    """PartitionResult encapsulates the processing of the messages for a single
    partition within a topic.

    The highest_committable should be set to the last message that was successfully
    handled during processing.  This allows the consumer to short circuit and commit
    a single message to cover successes, as anything before it is implicit.

    The PartitionResult also includes a breakdown of Messages:
        * succeeded - messages that were successfully processed
        * blocked - transient failures, that should be retried next poll
        * dead_letter - Messages that should be forwarded to a dead letter queue.
    """

    highest_committable: Message | None = None
    succeeded: list[Message] = field(default_factory=list)
    blocked: list[Message] = field(default_factory=list)
    dead_letter: list[Message] = field(default_factory=list)

    @property
    def all_success(self) -> bool:
        """success indicates if the entire batch was a success without any blocked
        or dead letter partitions"""
        return (
            bool(self.succeeded)
            and not bool(self.dead_letter)
            and not bool(self.blocked)
        )

    @property
    def should_dead_letter(self) -> bool:
        """should_dead_letter implies there were fatal failures in the batch
        and those should be treated as such."""
        return bool(self.dead_letter)

    @property
    def all_transient(self) -> bool:
        """all_transient implies all partitions are blocked but not in a fatal enough way
        to dead letter."""
        return (
            not bool(self.all_success)
            and not bool(self.dead_letter)
            and bool(self.blocked)
        )

    @property
    def all_dead_lettered(self) -> bool:
        """all_dead_lettered implies all partitions are blocked but not in a
        fatal way, messages require dead letter queueing."""
        return (
            bool(self.dead_letter)
            and not bool(self.blocked)
            and not bool(self.succeeded)
        )
