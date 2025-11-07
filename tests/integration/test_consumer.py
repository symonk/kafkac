import asyncio
import uuid
from mailbox import Message

import pytest

from kafkac import AsyncKafkaConsumer
from kafkac.handler import PartitionResult


async def successful_test_handler(messages: list[Message]) -> PartitionResult:
    return PartitionResult(succeeded=[topic_partition for topic_partition in messages])


@pytest.mark.asyncio
async def test_multiple_topic_regex_subscription_works_correctly() -> None: ...


@pytest.mark.asyncio
async def test_consumer_handles_fully_filtered_batches_successfully() -> None: ...


@pytest.mark.asyncio
async def test_filter_funcs_are_handled_correctly() -> None: ...


@pytest.mark.asyncio
async def test_consumer_throttles_when_throughput_is_zero() -> None: ...


@pytest.mark.asyncio
async def test_parallel_tasks_are_spawned_correctly_for_many_topics() -> None: ...


@pytest.mark.asyncio
async def test_consumer_handles_successful_batch_correctly() -> None: ...


@pytest.mark.asyncio
async def test_consumer_handles_partial_batch_correctly() -> None: ...


@pytest.mark.asyncio
async def test_consumer_handles_rebalancing_gracefully() -> None: ...


@pytest.mark.asyncio
@pytest.mark.parametrize("run", range(3))
async def test_simple_container(run, fx_kafka, message_producer) -> None:
    bootstrap_config, container, topic = fx_kafka
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
