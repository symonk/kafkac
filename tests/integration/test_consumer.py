import asyncio
import json
import uuid
from mailbox import Message

import pytest

from kafkac import AsyncKafkaConsumer
from kafkac.handler import PartitionResult

from ..utils import get_committed_messages_for_topic


async def successful_test_handler(messages: list[Message]) -> PartitionResult:
    await asyncio.sleep(0.10)
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
async def test_consuming_million_messages() -> None: ...


@pytest.mark.asyncio
async def test_simple_container(fx_kafka, message_producer) -> None:
    admin_client, bootstrap_config, container, topic = fx_kafka
    message_producer(bootstrap_config=bootstrap_config, topic=topic.topic, count=5000)
    consumer_group_id = "basic-test"
    bootstrap_config["group.id"] = consumer_group_id
    consumer_config = {
        "bootstrap.servers": bootstrap_config.get("bootstrap.servers"),
        "group.id": str(uuid.uuid4()),
        "auto.offset.reset": "earliest",
    }

    done = False

    def statter(topic: str):
        async def stats_cb(json_str) -> None:
            data = json.loads(json_str)
            handled = await get_committed_messages_for_topic(data, topic)
            if handled == 5000:
                nonlocal done
                done = True

        return stats_cb

    consumer = AsyncKafkaConsumer(
        handler_func=successful_test_handler,
        batch_size=1000,
        topic_regexes=[topic.topic],
        config=consumer_config,
        poll_interval=0.1,
        blocking_commit=True,
        stats_callback=(100, statter(topic.topic)),
    )

    async def exit_when_successful():
        while not done:
            await asyncio.sleep(0.05)
        consumer.stop()

    await asyncio.gather(*(exit_when_successful(), consumer.consume()))
