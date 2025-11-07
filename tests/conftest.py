import logging
import time
import typing

import pytest
from confluent_kafka import KafkaError
from confluent_kafka import KafkaException
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient
from confluent_kafka.admin import NewTopic
from filelock import FileLock
from testcontainers.kafka import KafkaContainer

logger = logging.getLogger(__name__)

DEFAULT_PARTITIONS = 40


@pytest.fixture(scope="session")
def test_kafka(
    tmp_path_factory: pytest.TempPathFactory,
) -> typing.Generator[tuple[dict[str, typing.Any], KafkaContainer], None, None]:
    """test_kafka sets up a pytest-xdist aware kafka cluster for testing.
    tests should not use this fixture, but instead the others which reference
    this fixture internally.  This enables a cluster, tests should specify
    their own topic to narrow their testing but keep things reliable and fast

    This fixture uses a filelock so it will only execute once, regardless of if
    pytest-xdist is used or not.
    """
    root_tmp_dir = tmp_path_factory.getbasetemp().parent
    fn = root_tmp_dir / "data.json"
    with FileLock(str(fn) + ".lock"):
        with KafkaContainer().with_kraft() as kafka:
            connection = kafka.get_bootstrap_server()
            bootstrap_cfg = {"bootstrap.servers": connection}
            yield bootstrap_cfg, kafka


@pytest.fixture(scope="function")
def fx_kafka(
    request: pytest.FixtureRequest, test_kafka
) -> typing.Generator[
    tuple[dict[str, typing.Any], KafkaContainer, NewTopic], None, None
]:
    bootstrap_cfg, kafka = test_kafka
    topic = getattr(
        request,
        "param",
        NewTopic(
            request.node.name.replace("[", "_").replace("]", "_"), DEFAULT_PARTITIONS
        ),
    )
    admin = AdminClient(bootstrap_cfg)
    admin.create_topics([topic])
    yield bootstrap_cfg, kafka, topic


# TODO: Mangling consumer/producer configs (group.id) etc which are ignored, tidy it up!
@pytest.fixture(scope="function")
def message_producer() -> typing.Callable[[dict[str, typing.Any], str, int], None]:
    def simple_producer(
        bootstrap_config: dict[str, typing.Any], topic: str, count: int = 120
    ) -> None:
        """simple_producer is a fixture that creates dummy data into kafka
        for testing the AsyncKafkaConsumer downstream."""
        bootstrap_config["message.send.max.retries"] = 1  # catch errors in test faster.
        start = time.monotonic()
        logger.info(f"producing {count} messages")

        def delivery_callback(err: KafkaError, msg):
            if err:
                raise Exception(f"failed publishing because: {err}")
            else:
                # all good, the message was published.
                ...

        try:
            p = Producer(**bootstrap_config)
            for _ in range(count):
                rand = f'"message": "{time.time_ns()}"'
                p.produce(topic, rand, callback=delivery_callback)
                p.poll(0)
            p.flush()
        except Exception as e:
            raise KafkaException(str(e)) from None

        logger.info(f"producing {count} messages in {time.monotonic() - start} seconds")

    return simple_producer
