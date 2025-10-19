import time
import typing

import pytest
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient
from confluent_kafka.admin import NewTopic
from testcontainers.kafka import KafkaContainer

DEFAULT_PARTITIONS = (
    40
)

@pytest.fixture(scope="function")
def test_kafka(request: pytest.FixtureRequest) -> typing.Generator[tuple[dict[str, typing.Any], KafkaContainer, NewTopic], None, None]:
    """kafka_cluster is a fixture that creates a new kafka cluster
    for each test that uses it.  The cluster contains a single topic
    named after the test node name.  The topic created can be configured
    by the test by passing indirect values through parameterization.

    If specified, these should be of type `NewTopic` types from the confluent kafka
    admin package.

    If the fixture is used without injecting any custom topic variables the topic will be named
    after the node name and the default partitions of 40 will be used.
    """
    topic = getattr(request, "param", NewTopic(request.node.name, DEFAULT_PARTITIONS))
    with KafkaContainer().with_kraft() as kafka:
        connection = kafka.get_bootstrap_server()
        bootstrap_cfg = {"bootstrap.servers": connection}
        admin = AdminClient(bootstrap_cfg)
        admin.create_topics([topic])
        yield bootstrap_cfg, kafka, topic


@pytest.fixture(scope="function")
def message_producer() -> typing.Callable[[dict[str, typing.Any], str, int], None]:
    def simple_producer(bootstrap_config: dict[str, typing.Any], topic: str, count: int = 120) -> None:
        """simple_producer is a fixture that creates dummy data into kafka
        for testing the AsyncKafkaConsumer downstream."""
        def delivery_callback(err, msg):
            if err:
                raise Exception("test producer failed publishing") from err
            else:
                # all good, the message was published.
                ...
        p = Producer(**bootstrap_config)
        for _ in range(count):
            rand = f'"message": "{time.time_ns()}"'
            p.produce(topic, rand, callback=delivery_callback)
            p.poll(0)
        p.flush()
    return simple_producer
