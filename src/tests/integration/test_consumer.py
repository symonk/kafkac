import asyncio
import pytest
import uuid
from kafkac import AsyncKafkaConsumer

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
    consumer = AsyncKafkaConsumer(topic_regexes=[topic.topic], librdkafka_config=consumer_config)

    async def stopper():
        await asyncio.sleep(3)
        consumer.stop()
    await asyncio.gather(*(stopper(), consumer.start()))
