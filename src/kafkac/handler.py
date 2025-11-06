import typing
from dataclasses import dataclass
from dataclasses import field

from confluent_kafka import Message


@dataclass
class BatchResult:
    """
    BatchResult should be returned by user defined handler coroutines.  This helps the core
    consumer loop to determine what to do.

    For messages where the offsets should be stored
    and later committed, successes should be set.

    For messages that failed for a transient reason, and should be retried later,
    store them in blocked and next poll will try them again.

    For messages that you deem to be fatal failures and should be treated as a dead letter
    scenario, store them in dead_letter.

    Note: The behaviour of dead_letter varies and is another point where the user can inject
    some behaviour.
    """

    success: list[Message] = field(default_factory=list)
    blocked: list[Message] = field(default_factory=list)
    dead_letter: list[Message] = field(default_factory=list)

    @property
    def all_success(self) -> bool:
        """success indicates if the entire batch was a success without any blocked
        or dead letter partitions"""
        return (
            bool(self.success) and not bool(self.dead_letter) and not bool(self.blocked)
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


# SingleMessageHandlerFunc accepts a single message, processes it and returns a result.
SingleMessageHandlerFunc = typing.Callable[[Message], typing.Awaitable[BatchResult]]
# BatchMessageHandlerFunc accepts a list of messages, from the same partition/topic, processes
# it and returns a result
BatchMessageHandlerFunc = typing.Callable[[list[Message]], typing.Awaitable[BatchResult]]
