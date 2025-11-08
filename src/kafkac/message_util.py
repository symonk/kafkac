from confluent_kafka import Message
from confluent_kafka import TopicPartition


def generate_offsets_from_kafka_messages(
    messages: list[Message],
) -> list[TopicPartition]:
    """generate_offsets_from_kafka_messages returns the offsets+1 for the
    batch of messages.  This allows storing offsets and performing a single
    commit() call per batch, amortizing the RTT to the brokers.  committing on
    a single message is extremely expensive and will kill throughput performance
    at the higher end.

    messages should be a list of `Message` objects that are not errors and are already
    validated as such, this function assumes all messages passed are considered 'successful'
    in the context of kafkac (simple success or dead letter success).

    IMPORTANT: The offsets returned here are successful offset + 1 as kafka expects the
    offset of the 'next in queue message', not the offset of the message that was just
    successfully processed.

    :returns: a list of `TopicPartition` objects, ready for storing.
    """
    offsets: list[TopicPartition] = []
    # TODO: Implement and update consumer to utilise it.
    return offsets
