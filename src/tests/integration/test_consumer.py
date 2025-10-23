import asyncio
import uuid
from mailbox import Message

import pytest

from kafkac import AsyncKafkaConsumer
from kafkac.handler import BatchResult


async def successful_test_handler(messages: list[Message]) -> BatchResult:
    return BatchResult(success=[topic_partition for topic_partition in messages])


@pytest.mark.asyncio
async def test_simple_container(test_kafka, message_producer) -> None:
    bootstrap_config, container, topic = test_kafka
    message_producer(bootstrap_config=bootstrap_config, topic=topic.topic, count=500)
    bootstrap_config["group.id"] = "basic-test"
    consumer_config = {
        "bootstrap.servers": bootstrap_config.get("bootstrap.servers"),
        "group.id": str(uuid.uuid4()),
        "auto.offset.reset": "earliest",
        "partition.assignment.strategy": "cooperative-sticky",
    }
    consumer = AsyncKafkaConsumer(
        handler_func=successful_test_handler,
        batch_size=1,
        topic_regexes=[topic.topic],
        librdkafka_config=consumer_config,
    )

    async def stopper():
        await asyncio.sleep(3)
        consumer.stop()

    await asyncio.gather(*(stopper(), consumer.start()))
