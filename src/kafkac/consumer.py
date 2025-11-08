import asyncio
import logging
import os
import time
import typing
from collections import defaultdict

from confluent_kafka import KafkaError
from confluent_kafka import KafkaException
from confluent_kafka import Message
from confluent_kafka import TopicPartition
from confluent_kafka.experimental.aio import AIOConsumer

from kafkac.filters.filter import FilterFunc

from .exception import InvalidHandlerFunctionException
from .exception import NoConsumerGroupIdProvidedException
from .handler import MessageHandlerFunc
from .handler import MessagesHandlerFunc
from .models import Batch
from .worker import batch_worker

# add a non-intrusive logger, allowing clients to view some useful information
# but not getting in their way.
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

# DEBUG_OPTS allow fine-grained control of librdkafka debugging logs to be
# emitted to the kafkac logger based on explicit values in the KAFKAC_DEBUG
# environ variable value.
DEBUG_OPTS = {
    "cgrp",
    "fetch",
    "topic",
    "consumer",
}


# TODO: This uses internal per message commit() calls, that is awfully slow, the RTT is per
# message, use store_offset and do a single commit per batch!
class AsyncKafkaConsumer:
    """
    AsyncKafkaConsumer is a fully asynchronously kafka consumer, ready for use
    out of the box. It is a little opinionated in some of the decisions it makes,
    these are outlined below, but it is worth noting, they are not set in stone
    and are very likely to change as the library evolves:

        * Enforced cooperative sticky rebalancing for incremental updates, preventing
        a stop-the-world rebalancing scenario.
        * Auto commit is disabled, user code should provide appropriate coroutines for
        handling the logic and should trust the AsyncKafkaConsumer to handle all scenarios
        gracefully, including rebalancing, dead lettering and transient vs non-transient
        error handling.

    The bare minimum required is to provide a coroutine handler for delegating the business
    logic of your application, this will be provided messages by the consumer.  Additionally,
    if a dead letter topic is provided in the initializer, kafkac will automatically detect
    poison-pill messages and dead letter them.  Kafkac is not opinionated on a dead letter
    queue scenario, should you choose multiple time based DLQ topics before a final store
    that is entirely upto the user, kafkac will only move messages onto the topic provided.

    A `group.id` must be provided in the options provided, this is fatal if not provided
    and an exception will be raised.

    In the future it will be possible to configure an entire DLQ config, where it may even be
    sending to another MSK in aws for example than the one consuming from the core topic(s).

    The `AsyncKafkaConsumer` only accepts keyword args for making backwards compatibility
    easier to manage in the future.

    The default algorithm of the consumer is as follows:
        * Fetch (upto) batch size of messages from kafka
        * Group messages into topic:partition ordered batches
            * Optionally throw away messages that should be filtered by filter_func
        * Process per topic, per partition batches in parallel, but synchronously within the batch
        * Depending on results, commit highest successful offsets
        * If anything is marked for dead lettering, produce the original message into the DLQ

    TODO: Document latency vs throughput scenarios and tuning.
    """

    def __init__(
        self,
        *,
        handler_func: MessageHandlerFunc | MessagesHandlerFunc,
        config: dict[str, typing.Any],
        batch_size: int,
        topic_regexes: list[str],
        poll_interval: float = 0.1,
        filter_func: FilterFunc | None = None,
        dlq_topic: str | None = None,
        batch_timeout: float = 60.0,  # TODO: Should probably be None if not specified.
        blocking_commit: bool = True,
        max_workers: int = min(32, (os.cpu_count() or 1) + 4),
        debug: bool = False,
        stats_callback: tuple[float, typing.Awaitable[str]] | None = None,
    ) -> None:
        if not isinstance(handler_func, MessageHandlerFunc | MessagesHandlerFunc):
            raise InvalidHandlerFunctionException(
                "type of handler_func must be `MessageHandlerFunc` or `MessagesHandlerFunc`"
            )

        # group.id is a required parameter
        if "group.id" not in config:
            raise NoConsumerGroupIdProvidedException(
                "consumer must be assigned a `group.id` in the librdkafka config"
            )

        # enable consumer level debugging logging, if explicitly set or `KAFKAC_DEBUG` is
        # in the environment.
        self.debug = debug or "KAFKAC_DEBUG" in os.environ
        # ensure a positive batch size, while also keeping it below the librdkafka limit of
        # 1M messages, if higher than this the core library will raise an error on consume(...)
        self.batch_size = min(max(batch_size, 1), 1_000_000)
        # handler_func allows the user to handle their business logic on a batch basis,
        # returning tri-state to the consumer (successes, to be retried, to be dead lettered).
        self.handler_func = handler_func
        # the core confluent_kafka asynchronous consumer.
        self.consumer: AIOConsumer | None = None
        # marks the consumer as running when start() is awaited.
        self.running = False
        # signals the consumer has been interrupted or `stop() is awaited.
        self.interrupted = False
        # the topic regexes that the consumer should subscribe too.
        self.topics_regexes = topic_regexes
        # the timeout to wait while trying to get a batch of messages.  If this timeout is exceeded
        # before the batch is full, a partial batch will be returned and processed.
        self.poll_interval = max(0.1, poll_interval)
        # an (optional) awaitable that is invoked for each message received.  If specified only messages
        # that return `True` will be processed by the consumer.  Returning `False` for a message will
        # cause the offset to be stored and ignored, future polls to the message buffer will move on
        # without processing.
        # Note: for multiple cases, build a wrapped composite function.
        # TODO: Probably need per topic filtering, or regex based options!
        self.filter_func = filter_func
        # an (optional) dead letter queue topic.  For now this only supports the same cluster
        # but will widen substantially in the future.
        self.dlq_func = dlq_topic
        # how many workers the thread pool can utilise when calling confluent kafka messages
        # that would block the event loop.
        # use the internal heuristic from std python, AIOConsumer does not expose it by default.
        self.workers = max_workers
        # keep track of the partitions assigned to this particular consumer
        # within the group.  Rebalance events can be common, rebalancing
        # is gracefully handled by the internals of the KafkaConsumer.
        # assigned partitions are topic specific, this tracks the topic name
        # to a set of partitions this consumer is currently responsible for.
        self.assigned_partitions: dict[str, set[int]] = defaultdict(set)
        # a fixed timeout for processing the batch, if 0 there is no timeout for
        # the batch.
        self.batch_timeout = float(max(batch_timeout, 0))
        # during rebalancing, it is important to prevent message processing while
        # callbacks are firing, especially true for revoking of partitions
        self.rebalance_lock = asyncio.Lock()
        # commits should be handled asynchronously by the librdkafka background thread.
        # this is non-blocking if set.
        self.blocking_commit = blocking_commit
        # a stats callback can be provided for now, later this will be overhauled to expose
        # useful concepts internally.
        # this should be provided as a tuple, in the form of (interval, callback) where the
        # client can expect (approximately) the callback to fire every interval.
        self.stats_callback = stats_callback
        # remove this later
        self.done = False
        # The core librdkafka configuration settings.
        # note: kafkac makes some strong opinions and overrides alot of configuration
        # see: _prepare and https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md
        self.librdkafka_config = self._prepare_cfg(config)

    def _prepare_cfg(
        self,
        user_cfg: dict[str, typing.Any],
    ) -> dict[str, typing.Any]:
        """TODO: Document"""
        user_cfg["enable.partition.eof"] = False
        user_cfg["enable.auto.commit"] = False
        user_cfg["enable.auto.offset.store"] = False
        if self.stats_callback:
            statistics_interval, stats_callback = self.stats_callback
            user_cfg["statistics.interval.ms"] = statistics_interval
            user_cfg.setdefault("stats_cb", stats_callback)
        # TODO: only enforce this if supporting a modern enough broker setup.
        # TODO: theres no image for this yet in docker, use classic for now.
        user_cfg["session.timeout.ms"] = 45000
        user_cfg["heartbeat.interval.ms"] = 55000
        # user_cfg["group.remote.assignor"] = "cooperative-sticky"
        # user_cfg["group.protocol"] = "consumer"
        user_cfg.setdefault("error_cb", self.error_cb)
        user_cfg.setdefault("throttle_cb", throttle_cb)
        # TODO: Allow the env var to specify exactly what to include if set.
        if self.debug:
            user_cfg.setdefault("debug", "consumer,cgrp,topic,fetch")
        return user_cfg

    async def consume(self) -> None:
        """consume signals the consumer to actually begin.  This is implicit
        when KafkaConsumer is used as a context manager."""
        try:
            self.consumer = AIOConsumer(
                consumer_conf=self.librdkafka_config, max_workers=self.workers
            )
            await self.consumer.subscribe(
                topics=self.topics_regexes,
                # TODO: Re-enable when docker tests work with KIP 848 on_assign=self._on_assign,
                # TODO: Re-enable when docker tests work with KIP 848 on_revoke=self._on_revoke,
                on_lost=self._on_lost,
            )
            self.running = True
            while not self.interrupted:
                # fetch a batch of messages from the subscribed topic(s).  Using consume
                # for batches is better for performance, as the async overhead is amortized
                # across the entire batch of messages.
                try:
                    messages = [
                        message
                        for message in await self.consumer.consume(
                            num_messages=self.batch_size, timeout=self.poll_interval
                        )
                        if message.error() is None
                    ]
                except KafkaError as err:
                    logger.error(err)
                    continue
                if not messages:
                    # Polling the broker for messages timed out without a message.
                    # The topic is possibly low traffic, or the producer may be
                    # slow or having an issue.  No need to sleep here to avoid a hot
                    # CPU loop, the consume call will delay this particular task.
                    await asyncio.sleep(0)  # Allow other tasks to run.
                    continue

                # filtered messages is the grouped messages, as in topic partition
                # ordered messages where messages that did not pass the filter are
                # removed.  The (optional) user supplied filter_func is applied to each message
                # and allows ignoring messages that do not meet the criteria.
                # by default, no messages are filtered.
                filtered_messages = await self._prepare_batch(messages)
                if not filtered_messages:
                    # the entire batch was 'filtered' out by the user.
                    # commit the entire batch and move on.
                    await self._store_offsets(messages)
                    continue

                logger.info("processing batch...")
                # squash the batch collected results, getting per topic partitions messages
                # in each of the list elements.
                squashed_partitions = [
                    messages
                    for partitions in filtered_messages.result.values()
                    for messages in partitions.values()
                ]
                tasks = [
                    asyncio.create_task(batch_worker(partition, self.handler_func))
                    for partition in squashed_partitions
                ]
                # as the tasks finish, store the successful offsets locally.
                for completed_task in asyncio.as_completed(tasks):
                    partition_result = await completed_task

                    # head of queue blocking (transient) is occurring on the partition.
                    # all the messages will be retried in a future poll.
                    if partition_result.all_transient:
                        continue

                    # the entire partitions messages were successful, store offsets for all.
                    if partition_result.all_success:
                        await self._store_offsets(partition_result.succeeded)
                        continue

                    # the entire partitions messages were dead lettered, store offsets for all.
                    if partition_result.all_dead_lettered:
                        # TODO: Do dead lettering!
                        await self._store_offsets(partition_result.dead_letter)
                        continue

                    # the more complicated scenario, we have partial failures within a single partition
                    # within the batch.  The `PartitionResult` keeps track of the highest offset that was
                    # successful, so we can just commit that, but only after dead letters were successful
                    # otherwise message loss can occur.
                    if partition_result.highest_committable is not None:
                        # TODO: Do dead lettering!
                        await self._store_offsets(
                            [partition_result.highest_committable]
                        )
                        continue

                try:
                    # the entire batch of messages has been handled.  commit the internally stored offsets
                    # once to amortize the RTT cost to the broker(s).
                    # The internal loop can store offsets internally at various different points.
                    # no offsets or messages are required here, instead it will commit anything
                    # internally stored.
                    committed_topic_partitions: (
                        list[TopicPartition] | None
                    ) = await self.consumer.commit(asynchronous=self.blocking_commit)
                    if committed_topic_partitions is None:
                        # There was commit failures.
                        ...
                    else:
                        # Some partitions possibly failed during commit.
                        # This might be mid re-balance, or broker network/IO errors etc.
                        # There is not much to be done, get visibility and retry next loop.
                        # Most errors are handling implicitly by librdkafka.
                        commit_failures: typing.DefaultDict[str, set[int]] = (
                            defaultdict(set)
                        )
                        for topic_partition in committed_topic_partitions:
                            if topic_partition.error is not None:
                                commit_failures[topic_partition.topic].add(
                                    topic_partition.partition
                                )

                        logger.error("some partitions failed")
                        ...

                except KafkaException as err:
                    logger.error(err)
                    continue

        # TODO: Exception handling.
        except KeyboardInterrupt:
            self.interrupted = True
            while not self.done:
                await asyncio.sleep(0.1)
        except Exception as e:
            logger.error(e)
        finally:
            # TODO: Remove logging, unsubscribing is slow tho...
            start = time.time()
            logger.info("Trying to shut down...")
            if self.running:
                # leave group and commit final offsets.
                await typing.cast(AIOConsumer, self.consumer).unsubscribe()
                await typing.cast(AIOConsumer, self.consumer).close()
                self.done = True
            logger.info(f"took {time.time() - start} seconds to shutdown")

    async def _commit(
        self,
        message: Message | None = None,
        offsets: list[TopicPartition] | None = None,
        block: bool = True,
    ) -> bool:
        """commit acks the stored offsets.

        TODO: Rewrite this logic, hacked for now."""
        commit_kw = {
            k: v
            for k, v in {
                "asynchronous": not block,
                "message": message,
                "offsets": offsets,
            }.items()
            if v is not None
        }

        results = await typing.cast(AIOConsumer, self.consumer).commit(**commit_kw)

        # asynchronous commit, a background librdkafka will handle the committing at some point
        # in the future.
        if results is None:
            return False

        # the topic/partitions are returned that were attempted, ensure all of them were marked as a success:
        # TODO: don't swallow the errors
        successes = [
            topic_partition
            for topic_partition in results
            if topic_partition.error is None
        ]
        if len(successes) == len(results):
            return True
        return False

    async def _store_offsets(self, messages: list[Message]) -> None:
        """internally store the offsets for successful messages."""
        for message in messages:
            try:
                await typing.cast(AIOConsumer, self.consumer).store_offsets(
                    message=message
                )
            except KafkaException as err:
                logger.error(err)

    async def _on_assign(
        self, _: AIOConsumer, partitions: list[TopicPartition]
    ) -> None:
        """on_assign retrieves the incremental partition updates.  The consumer
        can be multi-topic aware, so we need to keep track of per topic partitions."""
        async with self.rebalance_lock:
            for partition in partitions:
                topic, partition = partition.topic, partition.partition
                self.assigned_partitions[topic].add(partition)
        await typing.cast(AIOConsumer, self.consumer).incremental_assign(partitions)

    async def _on_revoke(
        self, _: AIOConsumer, partitions: list[TopicPartition]
    ) -> None:
        """_on_revoke is called during a rebalance when this particular consumer has
        lost some of it's previously owned partitions.  It should gracefully commit
        any offsets for these partitions to prevent message duplication etc when
        reassigned to another consumer in the group."""
        async with self.rebalance_lock:
            for partition in partitions:
                topic, partition = partition.topic, partition.partition
                self.assigned_partitions[topic].discard(partition)

        # commit anything stored already.
        await typing.cast(AIOConsumer, self.consumer).commit(
            asynchronous=not self.blocking_commit
        )
        await typing.cast(AIOConsumer, self.consumer).incremental_unassign(partitions)

    async def _on_lost(self, _: AIOConsumer, partitions: list[TopicPartition]) -> None:
        """on_lost is invoked when partitions owned by this particular consumer are considered
        lost.  This could be called when there is a failure in the coordinator etc.  At this
        point (unlike on_revoke) we no longer own the partitions when this is invoked internally."""
        async with self.rebalance_lock:
            for partition in partitions:
                topic, partition = partition.topic, partition.partition
                self.assigned_partitions[topic].discard(partition)

    @staticmethod
    async def error_cb(err: KafkaError) -> None:
        """error_cb is the default handle for global errors.  Importantly these
        errors are pretty much informative and no real action should need to be
        taken.  If the user does not specify one in their config, this will be
        used instead."""
        logger.error("received transient error: %s", err)

    async def _prepare_batch(self, messages: list[Message]) -> Batch:
        """_prepare_batch groups the messages which could span multiple topics into
        their post-filtered lists, retaining order of messages for the individual
        partitions.  The returned batch consists of many `GroupedMessages` which are
        per partition lists for each topic.

        If no filter_func was specified, all messages are included, otherwise messages
        are ignored that match the users filter criteria.  This allows iterating the batch
        messages only once, to group them and filter, ready for fanning out to workers
        for processing.
        """
        batch = Batch()
        for message in messages:
            if self.filter_func is not None:
                if await self.filter_func(message):
                    batch.store(message)
                else:
                    # TODO: Register an event system that ca be subscribed too rather than logging.
                    logger.debug(
                        "message dropped during filtering: %s:%d:%d",
                        message.topic,
                        message.partition,
                        message.offset,
                    )
            else:
                batch.store(message)
        return batch

    def stop(self) -> None:
        """stop signals that the consumer should begin a graceful shutdown.
        This will still allow in flight batches to be processed."""
        self.interrupted = True

    async def __aenter__(self) -> typing.Self:
        """__enter__ allows the KafkaConsumer instance to be used as a context
        manager, guaranteeing its graceful exit and teardown."""
        await self.consume()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await typing.cast(AIOConsumer, self.consumer).close()
        return None


async def throttle_cb() -> None:
    logger.debug("throttle_cb")
