import typing
import asyncio
from confluent_kafka.experimental.aio import AIOConsumer
from confluent_kafka import Message, TopicPartition


class AsyncKafkaConsumer:
    """KafkaConsumer wraps the confluent kafka consumer and offers improved
    developer experience and edge case handling automatically.

    # TODO: Testing
    # TODO: Signal handling and graceful shutdown
    # TODO: Document 'common' librdkafka settings (auto commit, acks etc)
    """

    def __init__(
        self,
        topic_regexes: list[str],
        librdkafka_config: dict[str, typing.Any],
        poll_interval: float = 0.1,
        filter_funcs: tuple[typing.Callable[..., ...], ...] = (),
        batch_timeout: float = 60.0,  # TODO: Should probably be None if not specified.
    ) -> None:
        if "group.id" not in librdkafka_config:
            raise ValueError("consumer must be assigned a `group.id` in the librdkafka config")
        self.consumer = None
        self.running = False
        self.interrupted = False
        self.topics_regexes = topic_regexes
        self.librdkafka_config = self._prepare_librdkafka_config(librdkafka_config)
        self.in_retry_state = {}
        self.poll_interval = max(0.1, poll_interval)
        self.filters = filter_funcs
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
        # a fixed timeout for processing the batch
        self.batch_timeout = float(max(batch_timeout, 1.0))
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
        return user_librdkafka_config

    async def start(self) -> None:
        """start signals the consumer to actually begin.  This is implicit
        when KafkaConsumer is used as a context manager."""
        try:
            self.consumer = AIOConsumer(consumer_conf=self.librdkafka_config, max_workers=self.workers)
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
                # TODO: make an algorithm here that knows when its missing messages
                # and auto scale it back, when messages are returned in the (potential)
                # batch, drop it down to near zero.
                messages = await self.consumer.consume(timeout=self.poll_interval)
                if not messages:
                    # Polling the broker for messages timed out without a message.
                    # The topic is possibly low traffic, or the producer may be
                    # slow or having an issue.  No need to sleep here to avoid a hot
                    # CPU loop, the consume call will delay this particular task.
                    continue

                # There are some messages to process, we can discard any messages that
                # do not adhere to the user supplied filter func coroutine.  Fan out
                # the message batch to that function to collect a grouping of messages
                # that actually apply, anything removed here should be automatically
                # move the offset forward on those particular partitions.
                filtered_messages = await self._handle_filters(messages)

                # we have filtered messages, group the messages by partition.  They are
                # passed to a worker pool by partition.  This allows asynchronous processing
                # of messages, but guaranteeing any failures for a particular partition within
                # the batch, that they are abandoned and the offset is not moved forward.
                # DLQ capabilities are supported to auto-detect poisonous messages that would
                # cause head of queue blocking and move them off to the side.
                # TODO: This is suboptimal, we are already filtering and iterating, we should provide
                # TODO: This as the output of that, consider for later.
                partitions_map = {}
                _ = partitions_map

                # for each of the partitions this consumed is assigned, fan out the partitions messages
                # as one transactional unit of work.  Should any message fail throughout processing the
                # entire batch is abandoned and the partition will be considered blocked, not stored and
                # will try again on the next consume loop.
                results: list[TopicPartition] = []
                await self._process_message(filtered_messages)

                # We have the results from the worker pool on a per partition basis
                # for cases where all messages in a partition were successful, move
                # the offset forward
                # TODO: Understand asynchronous commit here properly.
                await self.consumer.commit(
                    offsets=results, asynchronous=True
                )

        except KeyboardInterrupt:

            self.interrupted = True
            while not self.done:
                await asyncio.sleep(1)
        finally:
            if self.running:
                # leave group and commit final offsets.
                await self.consumer.unsubscribe()
                await self.consumer.close()

    async def _process_message(self, messages: list[Message]) -> None:
        """TODO: This will invoke client defined callables later."""
        for message in messages:
            print(message.value())

    async def _on_assign(self, _: AIOConsumer, partitions: list[TopicPartition]) -> None:
        self.assigned_partitions = set(topic.partition for topic in partitions)

    async def _on_revoke(self, _: AIOConsumer, partitions: list[TopicPartition]) -> None:
        """_on_revoke is called during a rebalance when this particular consumer has
        lost some of it's previously owned partitions.  It should gracefully commit
        any offsets for these partitions to prevent message duplication etc when
        reassigned to another consumer in the group."""
        await self.consumer.commit(offsets=partitions)

    async def _on_lost(self, _: AIOConsumer, partitions: list[TopicPartition]) -> None:
        """on_lost is invoked when partitions are lost unexpectedly """

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
