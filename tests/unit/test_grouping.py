from confluent_kafka import Message

from kafkac.grouping import group_messages_by_topic_partition


def test_grouping_by_topic_is_correct() -> None:
    messages = []
    topics = ["one", "two", "three"]  # order is important for verification.
    for topic in topics:
        messages.extend(Message(topic=topic, offset=i, partition=i) for i in range(10))
    output = group_messages_by_topic_partition(messages)
    # assert the (topic, partition) combinatory split is correct
    assert len(output.keys()) == 30
    for topic in topics:
        for i in range(10):
            key = (topic, i)
            assert len(output[key]) == 1
            out = output[key][0]
            assert out.topic() == topic
            assert out.partition() == i
            assert out.offset() == i
