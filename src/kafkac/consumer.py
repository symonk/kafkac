import typing
from confluent_kafka import Consumer
from confluent_kafka import KafkaError
from confluent_kafka import Message
import time
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
        topic_regexes: tuple[str, ...],
        librdkafka_config: dict[str, typing.Any],
        poll_interval: float = 1.0,
        filter_funcs: tuple[typing.Callable[..., ...], ...] = (),
    ) -> None:
        self.consumer = None
        self.running = True
        self.interrupted = False
        self.topics_regexes = topic_regexes
        self.librdkafka_config = librdkafka_config
        self.in_retry_state = {}
        self.poll_interval = max(poll_interval, 1.0)
        self.filters = filter_funcs

    def start(self) -> None:
        """start signals the consumer to actually begin.  This is implicit
        when KafkaConsumer is used as a context manager."""
        try:
            self.consumer = Consumer(self.librdkafka_config)
            # track the 'seen' partitions assigned to this particular consumer in
            # the group.
            seen_partitions = {}
            # track the partitions that are currently 'blocking' where message processing
            # is potentially failing and head of queue blocking for that particular log
            # stream is not moving forward.
            blocked_partitions = {}
            # TODO: What if topics do not exist etc.
            # TODO: Document topics can be regex based
            # TODO: Handle on_assign, on_revoke, on_lost etc
            self.consumer.subscribe(topics=self.topics_regexes, on_assign=self._offset_cb)
            while not self.interrupted:
                # TODO: make an algorithm here that knows when its missing messages
                # and auto scale it back, when messages are returned in the (potential)
                # batch, drop it down to near zero.
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
        except KeyboardInterrupt:
            self.interrupted = True
            while not self.done:
                time.sleep(1)
            # successfully gracefully shutdown and committed appropriate
            # offsets etc.
        finally:
            # leave group and commit final offsets.
            self.consumer.close()

    def _process_message(self, message: Message) -> None:
        if not self._handle_filters(message):
            return
        # No user defined filtering was applied to the message, we should
        # attempt to process it inline with other configurations of the
        # consumer, such as custom deserialisation etc.
        ...

    def _offset_cb(self) -> None: ...

    def _handle_filters(self, message: Message) -> bool:
        """_handle_filters is responsible for evaluating message headers against
        user defined behaviour to decide if the message should be even processed
        at all.  This allows filtering on particular versions, or where a header
        may dictate routing for particular environments etc that share the same
        AWS MSK etc.

        _handle_filters returns a boolean indicating if the message should be
        considered for processing.  If False, the process loop will discard the
        message but move the offset for that particular partition forward.
        """

    def __enter__(self) -> typing.Self:
        """__enter__ allows the KafkaConsumer instance to be used as a context
        manager, guaranteeing its graceful exit and teardown."""
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.consumer.close()
        return None
