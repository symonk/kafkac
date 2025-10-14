import typing
from confluent_kafka import Consumer


class KafkaConsumer:
    """KafkaConsumer wraps the confluent kafka consumer and offers improved
    developer experience and edge case handling automatically."""

    def __init__(
        self,
        topics: tuple[str, ...],
        librdkafka_config: dict[str, typing.Any],
    ) -> None:
        self.consumer = Consumer(**librdkafka_config)

    def start(self) -> None:
        """start signals the consumer to actually begin.  This is implicit
        when KafkaConsumer is used as a context manager."""

    def __enter__(self) -> typing.Self:
        """__enter__ allows the KafkaConsumer instance to be used as a context
        manager, guaranteeing its graceful exit and teardown."""
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.consumer.close()
        return None
