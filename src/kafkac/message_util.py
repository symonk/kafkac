from confluent_kafka import Message
from confluent_kafka import TopicPartition


# TODO: Move to somewhere else later
def get_highest_offset_per_partition(messages: list[Message]) -> list[TopicPartition]:
    tp = {}
    for message in messages:
        if message.partition in tp:
            if message.offset > tp[message.offset()]:
                tp[message.partition].append(
                    TopicPartition(
                        topic=message.topic(),
                        partition=message.partition(),
                        offset=message.offset(),
                    )
                )
        else:
            tp[message.partition] = TopicPartition(
                topic=message.topic(),
                partition=message.partition(),
                offset=message.offset(),
            )

    return list(tp.values())
