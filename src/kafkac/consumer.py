import typing
from confluent_kafka import Consumer
from confluent_kafka import KafkaError
from confluent_kafka import Message
from .exception import KafkacException


class KafkaConsumer:
    """KafkaConsumer wraps the confluent kafka consumer and offers improved
    developer experience and edge case handling automatically.

    # TODO: Testing
    # TODO: Signal handling and graceful shutdown
    # TODO: Document 'common' librdkafka settings (auto commit, acks etc)
    """

    def __init__(
        self,
        topics: tuple[str, ...],
        librdkafka_config: dict[str, typing.Any],
        poll_interval: float = 1.0,
    ) -> None:
        self.consumer = None
        self.running = True
        self.interrupted = False
        self.topics = topics
        self.librdkafka_config = librdkafka_config
        self.in_retry_state = {}
        self.poll_interval = max(poll_interval, 1.0)


    def start(self) -> None:
        """start signals the consumer to actually begin.  This is implicit
        when KafkaConsumer is used as a context manager."""
        self.consumer = Consumer(self.librdkafka_config)
        # TODO: What if topics do not exist etc.
        # TODO: Document topics can be regex based
        # TODO: Handle on_assign, on_revoke, on_lost etc
        self.consumer.subscribe(topics=self.topics, on_assign=self._offset_cb)
        while not self.interrupted:
            msg = self.consumer.poll(self.poll_interval)
            if msg is None:
                # Polling the broker for messages timed out without a message.
                # The topic is possibly low traffic, or the producer may be
                # slow or having an issue.
                continue
            if (err := msg.error()) is not None:
                if err.code() == KafkaError._PARTITION_EOF:
                    # the consumer assigned the partition is at the end of
                    # that particular partitions log.
                    continue
                # TODO: Allow this to be configurable, don't just raise and kill
                # the underlying consumer.
                raise KafkacException(err.message())
            else:
                # We have retrieved a successful message from the cluster
                try:
                    self._process_message(msg)
                    self.consumer.commit(msg, asynchronous=True)
                except KafkacException:
                    # TODO: Handle offset seeking etc.
                    ...


    def _process_message(self, message: Message) -> None:
        ...

    def _offset_cb(self) -> None:
        ...

    def __enter__(self) -> typing.Self:
        """__enter__ allows the KafkaConsumer instance to be used as a context
        manager, guaranteeing its graceful exit and teardown."""
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.consumer.close()
        return None
