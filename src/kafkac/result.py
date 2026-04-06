from dataclasses import dataclass
from dataclasses import field

from confluent_kafka import Message

from .exception import PoisonedMessagesWithNowhereToGoException


@dataclass
class KafkacContext:
    """KafkacContext is injected into user supplied handler function and is used to mark
    (at present) messages into three buckets:

     * successful - processed successfully and eligible for committing.
     * next-hop - failed and should be forwarded to a retry/dead-letter queue or store.
     * blocked - should head of queue block the partition, reseek it to retry the message next iteration.

    Note: `blocked` is not supported fully yet (and likely may be dropped as a concept).
    """

    topic: str
    messages: list[Message]
    hoppable: bool
    _successes: list[Message] = field(default_factory=list)
    _poisoned: list[Message] = field(default_factory=list)
    _blocked: list[Message] = field(default_factory=list)

    def mark_successful(self, message: Message) -> None:
        self._successes.append(message)

    def mark_poisoned(self, message: Message) -> None:
        if not self.hoppable:
            raise PoisonedMessagesWithNowhereToGoException(
                "cannot mark messages poisoned without configuring retry queues."
            )
        self._poisoned.append(message)

    def mark_blocked(self, message: Message) -> None:
        self._blocked.append(message)

    @property
    def all_success(self) -> bool:
        """success indicates if the entire batch was a success without any blocked
        or dead letter partitions"""
        return (
            bool(self._successes)
            and not bool(self._poisoned)
            and not bool(self._blocked)
        )

    @property
    def should_dead_letter(self) -> bool:
        """should_dead_letter implies there were fatal failures in the batch
        and those should be treated as such."""
        return bool(self._poisoned)
