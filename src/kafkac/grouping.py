from collections import defaultdict

from confluent_kafka import Message


def group_messages_by_topic_partition(
    messages: list[Message],
) -> dict[tuple[str, int], list[Message]]:
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
    return topic_partition_combinations

def group_messages_by_topic(messages: list[Message]) -> dict[str, list[Message]]:
    """group_messages_by_topic splits the array of kafka messages into batches
    based on the topic only.  This results in mixed partitions being in the same batch.

    Order within each partition should in theory be maintained, but if you have strict ordering
    guarantees you should consider using (topic, partition) to get an invocation of the handler
    for each of those combinations."""
    topics = defaultdict(list)
    for message in messages:
        topics[message.topic()].append(message)
    return topics


GroupRegistry = {
    "partition": group_messages_by_topic_partition,
    "topic": group_messages_by_topic,
}
