import asyncio
import typing
from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass

from confluent_kafka import Message

from kafkac import KafkacContext
from kafkac.worker import process_batch


@dataclass
class WrappedTask:
    """WrappedTask wraps an asyncio task but also includes metadata linked to that task for
    inspection later when joining/awaiting task completion."""

    task: asyncio.Task
    context: KafkacContext


class TaskGenerator(typing.Protocol):
    """TaskGenerate is the interface by which a strategy can create and
    dispatch asyncio tasks for the configured task_mode."""

    def __init__(self, task_mode: str, hoppable: bool, handler: typing.Any): ...

    def handle(self, messages: list[Message]) -> list[WrappedTask]: ...


class TaskModeStrategy:
    """TaskModeStrategy encapsulates the functions required for supporting
    different `task_mode` style strategies.  These are responsible for
    splitting messages into their respect combinations (prior to fanning out)
    the processing as well as providing the function for instantiating the
    asyncio tasks to be fanned out."""

    grouping: Callable[list[Message]]
    task_generator: Callable[list[Message]]


class TopicStrategy:
    """TopicStrategy groups the messages into topic combinations, where a single asyncio
    task is spawned for each individual topic the consumer is subscribed too, regardless of
    the number of partitions returned by a poll()."""

    def __init__(self, task_mode: str, hoppable: bool, handler: typing.Any):
        self.task_mode = task_mode
        self.hoppable = hoppable
        self.handler = handler

    def handle(self, messages: list[Message]) -> list[WrappedTask]:
        tasks: list[WrappedTask] = []

        # group messages into per topic combinations and create tasks for them.
        # attach appropriate metadata
        grouped_messages: list[list[Message]] = group_messages_by_topic(messages)

        return tasks


class PartitionStrategy:
    """PartitionStrategy groups the messages into tasks based on (topic, partition) combination
    resulting in an asyncio task spawned for each (topic, partition) assigned to this particular
    consumer."""

    def __init__(self, task_mode: str, hoppable: bool, handler: typing.Any) -> None:
        self.task_mode = task_mode
        self.hoppable = hoppable
        self.handler = handler

    def handler(self, messages: list[Message]) -> list[WrappedTask]:
        tasks: list[WrappedTask] = []

        # group messages into per topic combinations and create tasks for them.
        # attach appropriate metadata
        grouped_messages: list[list[Message]] = group_messages_by_topic_partition(
            messages
        )
        for messages in grouped_messages:
            context = KafkacContext(
                messages=messages, hoppable=self.hoppable, topic=messages[0].topic()
            )
            tasks.append(
                WrappedTask(
                    context=context,
                    task=asyncio.create_task(
                        process_batch(
                            context=context, messages=messages, handler=self.handler
                        )
                    ),
                )
            )
        return tasks


def group_messages_by_topic_partition(
    messages: list[Message],
) -> list[list[Message]]:
    """group_messages_by_topic_partition splits the array of kafka messages into
    (topic, partition) tuples, ready for dispatching to async tasks within the consumer.
    The user provided handler function will be passed the messages for each (topic, partition).

    This function does not concern itself with validation messages are valid, such as they are not
    error types yielded from kafka, the consumer itself guarantees that.

    This function is provided the messages polled from kafka, which should retain order and iterates
    them in the order they are in the original messages array, thus they are correctly handled to
    retain the offset order as expected.
    """
    topic_partition_combinations = defaultdict(list)
    for message in messages:
        key = (message.topic(), message.partition())
        topic_partition_combinations[key].append(message)
    return [v for v in topic_partition_combinations.values()]


def group_messages_by_topic(messages: list[Message]) -> list[list[Message]]:
    """group_messages_by_topic splits the array of kafka messages into batches
    based on the topic only.  This results in mixed partitions being in the same batch.

    Order within each partition should in theory be maintained, but if you have strict ordering
    guarantees you should consider using (topic, partition) to get an invocation of the handler
    for each of those combinations."""
    topics = defaultdict(list)
    for message in messages:
        topics[message.topic()].append(message)
    return [v for v in topics.values()]


GroupRegistry = {
    "partition": group_messages_by_topic_partition,
    "topic": group_messages_by_topic,
}

TaskModeRegistry: dict[str, TaskGenerator] = {
    "partition": PartitionStrategy,
    "topic": TopicStrategy,
}
