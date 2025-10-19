import asyncio
import pytest
from kafkac import AsyncKafkaConsumer

@pytest.mark.asyncio
async def test_simple_container(test_kafka, message_producer) -> None:
    bootstrap_config, container, topic = test_kafka
    message_producer(bootstrap_config=bootstrap_config, topic=topic.topic, count=500)
    bootstrap_config["group.id"] = "basic-test"
    consumer = AsyncKafkaConsumer(topic_regexes=(topic.topic,), librdkafka_config=bootstrap_config)

    async def stopper():
        await asyncio.sleep(5)
        consumer.stop()
    await asyncio.gather(*(stopper(), consumer.start()))
