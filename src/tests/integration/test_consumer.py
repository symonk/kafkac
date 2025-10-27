import asyncio
import uuid
from mailbox import Message

import pytest

from kafkac import AsyncKafkaConsumer
from kafkac.handler import BatchResult


async def successful_test_handler(messages: list[Message]) -> BatchResult:
    return BatchResult(success=[topic_partition for topic_partition in messages])


@pytest.mark.asyncio
@pytest.mark.parametrize("run", range(3))
async def test_simple_container(run, test_topic, message_producer) -> None:
    bootstrap_config, container, topic = test_topic
    message_producer(bootstrap_config=bootstrap_config, topic=topic.topic, count=5000)
    bootstrap_config["group.id"] = "basic-test"
    consumer_config = {
        "bootstrap.servers": bootstrap_config.get("bootstrap.servers"),
        "group.id": str(uuid.uuid4()),
        "auto.offset.reset": "earliest",
    }
    consumer = AsyncKafkaConsumer(
        handler_func=successful_test_handler,
        batch_size=1000,
        topic_regexes=[topic.topic],
        config=consumer_config,
    )

    async def stopper():
        await asyncio.sleep(3)
        consumer.stop()

    await asyncio.gather(*(stopper(), consumer.start()))