import pytest
from kafkac import AsyncKafkaConsumer

@pytest.mark.asyncio
async def test_simple_container(test_kafka, callable_producer) -> None:
    bootstrap_config, container, topic = test_kafka
    callable_producer(bootstrap_config=bootstrap_config, topic=topic.topic, count=500)
    consumer = AsyncKafkaConsumer(topic_regexes=topic.topic, librdkafka_config=bootstrap_config)
    await consumer.start()
