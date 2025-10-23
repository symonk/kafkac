import asyncio
import logging
import typing

from confluent_kafka import Message
from confluent_kafka import TopicPartition
from confluent_kafka.experimental.aio import AIOConsumer

from .filter import FilterFunc
from .handler import BatchResult
from .handler import HandlerFunc

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


class AsyncKafkaConsumer:
    """KafkaConsumer wraps the confluent kafka consumer and offers improved
    developer experience and edge case handling automatically.

    # TODO: Testing
    # TODO: Signal handling and graceful shutdown
    # TODO: Document 'common' librdkafka settings (auto commit, acks etc)
    """

    def __init__(
        self,
        handler_func: HandlerFunc,
        batch_size: int,
        topic_regexes: list[str],
        librdkafka_config: dict[str, typing.Any],
        poll_interval: float = 0.1,
        filter_func: FilterFunc | None = None,
        dlq_func: DQLFunc | None = None,
        batch_timeout: float = 60.0,  # TODO: Should probably be None if not specified.
    ) -> None:
        # group.id is a required parameter
        if "group.id" not in librdkafka_config:
            raise ValueError(
                "consumer must be assigned a `group.id` in the librdkafka config"
            )
        # ensure a positive batch size, while also keeping it below the librdkafka limit of
        # 1M messages, if higher than this the core library will raise an error on consume(...)
        self.batch_size = min(max(batch_size, 1), 1_000_000)
        self.handler_func = handler_func
        self.consumer = None
        self.running = False
        self.interrupted = False
        self.topics_regexes = topic_regexes
        self.librdkafka_config = self._prepare_librdkafka_config(librdkafka_config)
        self.in_retry_state = {}
        self.poll_interval = max(0.1, poll_interval)
        self.filter_func = filter_func
        self.dlq_func = dlq_func
        # track `done` which signals after interruption, the finalizers are complete and it is safe
        # to fully close out the consumer.
        self.done = False
        self.workers = 8  # TODO: Derive this, or make it available to users.
        # keep track of the partitions assigned to this particular consumer
        # within the group.  Rebalance events can be common, rebalancing
        # is gracefully handled by the internals of the KafkaConsumer.
        # assigned partitions are topic specific, this tracks the topic name
        # to a set of partitions this consumer is currently responsible for.
        self.assigned_partitions = dict[str, set[int]]
        # a fixed timeout for processing the batch, if 0 there is no timeout for
        # the batch.
        self.batch_timeout = float(max(batch_timeout, 0))
        # during rebalancing, it is important to prevent message processing while
        # callbacks are firing, especially true for revoking of partitions
        self.rebalance_lock = asyncio.Lock()

    @staticmethod
    def _prepare_librdkafka_config(
        user_librdkafka_config: dict[str, typing.Any],
    ) -> dict[str, typing.Any]:
        """
        TODO: Implement, we should probably enforce auto commit offset off, and maybe some others.
        :return:
        """
        user_librdkafka_config["enable.partition.eof"] = False
        user_librdkafka_config["enable.auto.commit"] = False
        user_librdkafka_config["enable.auto.offset.store"] = False
        user_librdkafka_config["stats_cb"] = stats_cb
        return user_librdkafka_config

    async def start(self) -> None:
        """start signals the consumer to actually begin.  This is implicit
        when KafkaConsumer is used as a context manager."""
        try:
            self.consumer = AIOConsumer(
                consumer_conf=self.librdkafka_config, max_workers=self.workers
            )
            # TODO: What if topics do not exist etc.
            # TODO: Document topics can be regex based
            # TODO: Handle on_assign, on_revoke, on_lost etc
            await self.consumer.subscribe(
                topics=self.topics_regexes,
                on_assign=self._on_assign,
                on_revoke=self._on_revoke,
                on_lost=self._on_lost,
            )
            self.running = True
            while not self.interrupted:
                # fetch a batch of messages from the subscribed topic(s).  Using consume
                # for batches is better for performance, as the async overhead is amortized
                # across the entire batch of messages.
                messages = await self.consumer.consume(
                    num_messages=self.batch_size, timeot=self.poll_interval
                )
                if not messages:
                    # Polling the broker for messages timed out without a message.
                    # The topic is possibly low traffic, or the producer may be
                    # slow or having an issue.  No need to sleep here to avoid a hot
                    # CPU loop, the consume call will delay this particular task.
                    continue

                # for each of the messages, apply a user defined filter (if specified)
                # otherwise do not apply filtering at all and process all messages.
                # this is useful for cases where a kafka header may prevent a lot of deserialisation
                # in the core process loop to save sizable on time and increase throughput.
                filtered_messages = await self._handle_filters(messages)
                if len(filtered_messages) == 0:
                    # the entire batch was 'filtered' out by the user.
                    # TODO: handle exceptions (RuntimeError/KafkaException)
                    await self._commit(message=None, offsets=None, block=True)
                    continue

                # for each of the partitions this consumed is assigned, fan out the partitions messages
                # as one transactional unit of work.  Should any message fail throughout processing the
                # entire batch is abandoned and the partition will be considered blocked, not stored and
                # will try again on the next consume loop.
                batch_result: BatchResult = await self.handler_func(filtered_messages)
                if not isinstance(batch_result, BatchResult):
                    raise ValueError("handler coroutines must return a `BatchResult`")

                # every partition is blocked in a transient way, next loop the same batch of messages
                # will be retried, unless the batch was not full in which case a superset of this batch
                # will be retried.
                if batch_result.all_transient:
                    continue

                # inspect the batch results to derive offsets to store and commit or dead letter behaviour
                # to manage.
                # check if the entire batch was successful and commit all
                if batch_result.all_success:
                    await self._commit(message=None, offsets=None, block=True)
                    continue

                # check if there was dead letter fatal failures, for now hard coded printing them
                # dead letters are technically 'successful' and from the consumers point of view
                # should roll the offset forward.
                if batch_result.dead_letter:
                    logger.info("all dead lettered!")
                    await self._commit(message=None, offsets=None, block=True)

                # the most complicated scenario, the batch had a mix of successes and failures
                # within it.  The lowest successful offset of each partition will be moved forward
                # to avoid message loss, anything that was successful after a failed offset prior
                # will be retried, causing duplication.  In future kafkac will be much smarter in this
                # scenario.
                raise ValueError("not implemented yet")

        except KeyboardInterrupt:
            self.interrupted = True
            while not self.done:
                await asyncio.sleep(1)
        finally:
            if self.running:
                # leave group and commit final offsets.
                await self.consumer.unsubscribe()
                await self.consumer.close()

    async def _commit(
        self,
        message: Message | None = None,
        offsets: list[TopicPartition] | None = None,
        block: bool = True,
    ) -> bool:
        """commit attempts to store the offsets

        TODO: Rewrite this logic, hacked for now."""
        results = await self.consumer.commit(
            message=message, offsets=offsets, asynchronous=not block
        )

        # asynchronous commit, a background librdkafka will handle the committing at some point
        # in the future.
        if results is None:
            return False

        # the topic/partitions are returned that were attempted, ensure all of them were marked as a success:
        # TODO: don't swallow the errors
        successes = [
            topic_partition
            for topic_partition in results
            if topic_partition.error() is None
        ]
        if len(successes) == len(results):
            return True
        return False

    # TODO: on_assign & on_revoke need to use incremental assign.
    async def _on_assign(
        self, _: AIOConsumer, partitions: list[TopicPartition]
    ) -> None:
        self.assigned_partitions = set(topic.partition for topic in partitions)

    async def _on_revoke(
        self, _: AIOConsumer, partitions: list[TopicPartition]
    ) -> None:
        """_on_revoke is called during a rebalance when this particular consumer has
        lost some of it's previously owned partitions.  It should gracefully commit
        any offsets for these partitions to prevent message duplication etc when
        reassigned to another consumer in the group."""
        await self.consumer.commit(offsets=partitions)

    async def _on_lost(self, _: AIOConsumer, partitions: list[TopicPartition]) -> None:
        """on_lost is invoked when partitions are lost unexpectedly"""

    async def _handle_filters(self, messages: list[Message]) -> list[Message]:
        """_handle_filters is responsible for evaluating message headers against
        user defined behaviour to decide if the message should be even processed
        at all.  This allows filtering on particular versions, or where a header
        may dictate routing for particular environments etc that share the same
        AWS MSK etc.

        _handle_filters returns a boolean indicating if the message should be
        considered for processing.  If False, the process loop will discard the
        message but move the offset for that particular partition forward.
        """
        applicable: list[Message] = []
        for message in messages:
            if err := message.error():
                logger.error("message error in the batch: %s", err)
            if await self.filter_func(message):
                applicable.append(message)
            else:
                logger.debug(
                    "message dropped during filtering: %s:%d:%d",
                    message.topic,
                    message.partition,
                    message.offset,
                )

        return messages

    def stop(self) -> None:
        """stop signals that the consumer should begin a graceful shutdown.
        This will still allow in flight batches to be processed."""
        self.interrupted = True

    def __enter__(self) -> typing.Self:
        """__enter__ allows the KafkaConsumer instance to be used as a context
        manager, guaranteeing its graceful exit and teardown."""
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.consumer.close()
        return None


async def stats_cb() -> None: ...


async def throttle_cb() -> None: ...


async def error_cb() -> None: ...
