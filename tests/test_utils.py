"""utils.py houses various different utilities only useful within
tests, but extremely important to keep tests DRY and verify kafka
behaviour etc."""


async def get_committed_messages_for_topic(statistics: dict, topic: str) -> int:
    """get_committed_messages_for_topic retrieves the successfully
    committed messages for all partitions on a particular topic.

    This allows verifying that the consumer has actually consumed the
    correct number of messages within tests.
    """
    topics = statistics["topics"][topic]["partitions"]
    handled = sum(v["committed_offset"] for k, v in topics.items() if k != "-1")
    return handled
