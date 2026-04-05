import asyncio
import logging
import os
import typing
from collections import defaultdict

from confluent_kafka import KafkaError
from confluent_kafka import KafkaException
from confluent_kafka import Message
from confluent_kafka import TopicPartition
from confluent_kafka.aio import AIOConsumer

from kafkac.filters import FilterFunc
from kafkac.filters import discard_message

from .debug import parse_debug_options
from .exc_handler import BatchExcHandler
from .exception import InvalidHandlerFunctionException
from .exception import NoConsumerGroupIdProvidedException
from .exception import PoisonedMessagesWithNowhereToGoException
from .exception import UnsupportedMessagingGroupException
from .grouping import GroupRegistry
from .handler import MessagesHandlerFunc
from .result import HandlerResultContext
from .retry import RetryConfig
from .retry import RetryRouter
from .worker import BatchedWrappedUnhandledException
from .worker import process_batch

# add a non-intrusive logger, allowing clients to view some useful information
# but not getting in their way if they do not specify their own user_logger.
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

# An alias for the topic, or topic partition fan out strategies.
GroupedMessagesType = dict[tuple[str, int], list[Message]] | dict[str, list[Message]]


class AsyncKafkaConsumer:
    """
    AsyncKafkaConsumer is a fully asynchronously kafka consumer, ready for use
    out of the box. It is a little opinionated in some of the decisions it makes,
    these are outlined below, but it is worth noting, they are not set in stone
    and are very likely to change as the library evolves:

        * New generation consumer rebalance consumer, preventing stop-the-world semantics
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
        handler_func: MessagesHandlerFunc,
        config: dict[str, typing.Any],
        batch_size: int,
        topic_regexes: list[str],
        poll_interval: float = 0.1,
        filter_funcs: dict[str, list[FilterFunc]] | None = None,
        retry_cfg: RetryConfig | None = None,
        batch_timeout: float = 60.0,  # TODO: Should probably be None if not specified.
        async_commit: bool = False,
        max_workers: int = min(32, (os.cpu_count() or 1) + 4),
        debug: str | None = None,
        stats_callback: tuple[float, typing.Awaitable[str]] | None = None,
        consumer_logger: logging.Logger = logger,
        bound_concurrency: int = 0,
        task_mode: typing.Literal["topic", "partition"] = "partition",
        batch_exc_handler: BatchExcHandler = BatchExcHandler(
            retries=0,
            on=tuple(),
        ),
    ) -> None:
        if not isinstance(handler_func, MessagesHandlerFunc):
            raise InvalidHandlerFunctionException(
                "type of handler_func must be `MessageHandlerFunc` or `MessagesHandlerFunc`"
            )

        # group.id is a required parameter
        if "group.id" not in config:
            raise NoConsumerGroupIdProvidedException(
                "consumer must be assigned a `group.id` in the librdkafka config"
            )

        # enable consumer level debugging, these will also be written to the provided logger
        # if specified.  Values should be a comma separated list of values, the supported
        # consumer options are: consumer,cgrp,topic,fetch
        # WARNING: This can be noisy!
        self.debug = debug or os.environ.get("KAFKA_CONFIG", "")
        # ensure a positive batch size, while also keeping it below the librdkafka limit of
        # 1M messages, if higher than this the core library will raise an error on consume(...)
        self.batch_size = min(max(batch_size, 1), 1_000_000)
        # handler_func allows the user to handle their business logic on a batch basis,
        # returning tri-state to the consumer (successes, to be retried, to be dead lettered).
        self.handler_func = handler_func
        # marks the consumer as running when start() is awaited.
        self.running = False
        # signals the consumer that `stop()` has been called.
        self.interrupted = False
        # the topic regexes that the consumer should subscribe too.
        self.topics_regexes = topic_regexes
        # the timeout to wait while trying to get a batch of messages.  If this timeout is exceeded
        # before the batch is full, a partial batch will be returned and processed.
        # indefinite polling is not supported (-1) because it can lead to segfaults when interrupted
        # etc (attempting to close a consumer) that is stuck in the poll code.
        self.poll_interval = max(0.1, poll_interval)
        # An (optional) topic specific list of filter funcs.  Filter funcs allow inspecting
        # kafka headers to discard messages without full deserialisation of the body which
        # is often costly.  At present regex on topics is not an option and topics are exactly
        # matched.  filter funcs should be provided as a list of `FilterFunc` and these are
        # executed in FIFO order for the topic.
        self.filter_funcs = filter_funcs
        # an (optional) dead letter queue topic.  For now this only supports the same cluster
        # but will widen substantially in the future.
        self.retrier = RetryRouter(retry_cfg=retry_cfg) if retry_cfg else None
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
        self.async_commit = async_commit
        # a stats callback can be provided for now, later this will be overhauled to expose
        # useful concepts internally.
        # this should be provided as a tuple, in the form of (interval, callback) where the
        # client can expect (approximately) the callback to fire every interval.
        self.stats_callback = stats_callback
        # The logger to use.  if the user provides their own logger it will be used, otherwise
        # the internal kafkac logger will be used.
        # Note: This must be set before _prepare_cfg is invoked.
        self.consumer_logger = consumer_logger
        # if the use case has alot of (topic, partitions) and you wish to potentially not overwhelm
        # downstream systems, setting bound_concurrency will limit (via a semaphore) the number of
        # (topic, partition) tasks in flight any given time.  By default, the tasks are unbound and
        # will attempt to execute all in parallel.
        self.bound_concurrency = bound_concurrency
        # Allows configuring how the batch handler provided is invoked.  Two options are supported:
        # topic -> The batch handler will be awaited for each (topic).  This means, 'mixed' partitions.
        # partition -> The batch handler will be awaited for each (topic, partition) combination (default).
        if task_mode not in {"topic", "partition"}:
            raise UnsupportedMessagingGroupException(
                f"task_mode {task_mode!r} is not supported"
            )
        self.task_mode = task_mode
        # assign the function responsible for delegating the kafka messages polled into their
        # appropriate (topic) or (topic, partition) groups.
        # This is influenced by `task_mode`.
        self._message_grouper_func = GroupRegistry[self.task_mode]
        # configure behaviour if an unhandled exception leaks out of the batch handler.
        self.batch_exc_handler = batch_exc_handler

        # -- Order is important below here, at least temporarily, do not append attributes until fixed --

        # The core librdkafka configuration settings.
        # note: kafkac makes some strong opinions and overrides alot of configuration
        # see: _prepare and https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md
        self.librdkafka_config = self._prepare_cfg(config)
        # the core confluent_kafka asynchronous consumer.
        # note: this must be instantiated last, since it is blocking and begins
        # the core loop.
        self.consumer: AIOConsumer = AIOConsumer(
            consumer_conf=self.librdkafka_config,
            max_workers=self.workers,
        )
        # track when the consumer.consume loop has finalized and is finished/exited.
        self.done = False

    def _prepare_cfg(
        self,
        user_cfg: dict[str, typing.Any],
    ) -> dict[str, typing.Any]:
        """TODO: Document"""
        user_cfg["enable.auto.commit"] = False
        user_cfg["enable.auto.offset.store"] = False
        if self.stats_callback:
            statistics_interval, stats_callback = self.stats_callback
            user_cfg["statistics.interval.ms"] = statistics_interval
            user_cfg.setdefault("stats_cb", stats_callback)
        if self.consumer_logger:
            user_cfg["logger"] = self.consumer_logger
        # explicitly opt in to the KIP-848 (Next generation consumer)
        # TODO: We can't enforce this really, it requires broker side config
        # TODO: Let's make it 'optional', if the user wants to opt in - use it.
        # user_cfg["group.protocol"] = "consumer"
        user_cfg.setdefault("error_cb", self.error_cb)
        if options := parse_debug_options(self.debug):
            user_cfg.setdefault("debug", options)
        return user_cfg

    async def consume(self) -> None:
        """consume signals the consumer to actually begin.  This is implicit
        when KafkaConsumer is used as a context manager."""
        try:
            try:
                await self._subscribe()
            except KafkaException as exc:
                self._log_kafka_exception(exc)
                raise  # (TODO: Crash)

            self.running = True

            while not self.interrupted:
                # fetch a batch of messages from the subscribed topic(s).  Using consume
                # for batches is better for performance, as the async overhead is amortized
                # across the entire batch of messages.
                try:
                    messages = [
                        message
                        for message in await self.consumer.consume(
                            num_messages=self.batch_size,
                            timeout=self.poll_interval,
                        )
                        if message.error() is None
                    ]
                except KafkaException as exc:
                    self._log_kafka_exception(exc)
                    raise  # (TODO: Crash)

                if not messages:
                    # Polling the broker for messages timed out without a message.
                    # The topic is possibly low traffic, or the producer may be
                    # slow or having an issue.  No need to sleep here to avoid a hot
                    # CPU loop, the consume call will delay this particular task.
                    self.consumer_logger.info(
                        "no more messages"
                    )  # (TODO: Debugging - Remove)
                    continue

                # apply (optional) user derived filtering, which allows dropping messages
                # based on kafka headers etc. prior to parsing the full payload.  These
                # filters skip messages, retaining those that are 'applicable' for further
                # processing.  Filters are provided on a `per-topic` basis and regex is not
                # currently supported (yet).
                # TODO: Abstract into _filter_msgs func
                applicable_messages = messages if not self.filter_funcs else []
                if self.filter_funcs:
                    for message in messages:
                        topic = message.topic()
                        awaitables = self.filter_funcs.get(topic)
                        if not awaitables:
                            # there are registered 'filters' for this topic
                            exclude = await discard_message(topic, message, awaitables)
                            if not exclude:
                                applicable_messages.append(exclude)
                        else:
                            applicable_messages.append(message)

                if not applicable_messages:
                    # the entire batch was 'filtered' out by the user.
                    # safe to store and commit all before continuing.
                    # TODO: commit() is enough in these cases, no need to store, pass partitions to commit()
                    # (TODO: Crash)
                    try:
                        await self._store_messages(messages)
                        await self._commit(asynchronous=self.async_commit)
                    except KafkaException as exc:
                        self._log_kafka_exception(exc)
                        raise
                    else:
                        continue

                # based on user configuration, group the messages into either
                # a dict of topic: list[Message] OR
                # a dict of (topic, partition): list[Message]
                grouped_messages: GroupedMessagesType = self._message_grouper_func(
                    applicable_messages
                )

                # TODO: This does not honour task_mode, its always (topic, partition) atm.
                topic_partition_results = await self._process(grouped_messages)

                # process the user batch handler results, enforcing complex logic to derive the
                # blocked, successful and poisoned offsets that can actually be stored.
                # Note: For developers changing these concepts, be very careful - the decision tree is very nuanced
                # and is extremely prone to mistakes, resulting in disastrous outcomes for users.
                #
                # blocked_partitions denotes which partitions should be 'blocked', if there are any blocked
                # partitions, kafkac will sort the offsets for the partition up until the blocked offset.
                # those will be `stored`, and the 'blocked' case will be 're-seeked'.  What this means is,
                # next polls() will return those messages AGAIN.  Be careful when using this as it may not be
                # what you want.  The preferred approach would be to mark such messages as `poisoned` (below)
                # and have them enqueued somewhere else for processing in the future.  Using `blocked` here will
                # cause head-of-queue blocking on that partition, this may be desirable in some cases (such as
                # external systems completely down, that would result in mass dead lettering etc., but be very
                # careful that in how you make those decisions.

                # successful_partitions will have all their offsets stored and commited, kafkac will ensure no
                # blocked offsets are 'intermingled' within these (or poisoned messages) to ensure user error
                # does not result in message loss.

                # poisoned_messages denotes messages that are technically successful, but only if the action of
                # actually enqueueing them is successful.  If configured to move messages forward (for transient
                # failures) to something like a retry queue or DLQ, kafkac will attempt to publish them.  Initially
                # only a kafka topic will be supported, but future plugins will exist such as SQS.

                successful_partitions, blocked_partitions = await self._process_results(
                    topic_partition_results
                )

                if not successful_partitions and not blocked_partitions:
                    raise RuntimeError(
                        "Kafkac internal bug - please open an issue @ https://github.com/symonk/kafkac/issues"
                    )

                # At this point, successful and blocked partitions are accurate, it is the responsibility of
                # _process_results to ensure correct constraints are applied internally there.
                # reseek partitions that are marked as blocked (if any exists)
                # if we fail to reseek a partition, crash the consumer for now (TODO: Crash)
                for blocked_partition in blocked_partitions:
                    try:
                        await self.consumer.seek(blocked_partition)
                    except KafkaException as exc:
                        self._log_kafka_exception(exc)
                        raise

                # Finally commit the successful offsets, for an individual partition, the max offset here will
                # be no greater than either: A) The max of the initial polled messages (if no blocks occur) or
                # B) Less than the least offset for a block partition to prevent message loss.
                #
                # By this point, if next-hop is configured, those transient failures will either be:
                # A) Successfully enqueued, and included in successful_partitions.
                # B) Failed to enqueue and configured to crash the consumer.
                # C) Failed to enqueue and configured to HOQ block in which case they will be in blocked above.
                # TODO: commit() is sufficient here with offsets
                try:
                    await self._store_offsets(offsets=successful_partitions)
                    await self._commit(asynchronous=self.async_commit)
                except KafkaException as exc:
                    self._log_kafka_exception(exc)
                    raise  # (TODO: Crash)
        except Exception:
            raise
        finally:
            if self.running:
                # leave group and commit final offsets.
                await self.consumer.unsubscribe()
                await self.consumer.close()
                self.done = True

    async def _subscribe(self) -> None:
        """_subscribe subscribes the consumer to the regex based topics provided
        when the consumer was initialised."""
        try:
            await self.consumer.subscribe(
                topics=self.topics_regexes,
                on_assign=self._on_assign,
                on_revoke=self._on_revoke,
            )
        except KafkaException as exc:
            self._log_kafka_exception(exc)

    def _log_kafka_exception(self, exc: KafkaException) -> None:
        """_log_kafka_error unwraps a KafkaException and logs information about the
        underlying KafkaError.

        :param exc: The KafkaException (raised by kafka consumer operations).

        """
        err: KafkaError = exc.args[0]
        self.consumer_logger.error(
            "failed to subscribe to topics",
            extra={
                "topics": self.topics_regexes,
                "error": err.str(),
                "error_code": err.code(),
                "retriable": err.retriable(),
                "fatal": err.fatal(),
            },
        )

    async def _process(
        self,
        grouped_messages: GroupedMessagesType,
    ) -> list[HandlerResultContext | Exception]:
        """_process fans out the batches appropriately and collects results."""
        tasks: list[asyncio.Task] = []  # order is important here.
        for key, partition_messages in grouped_messages.items():
            topic, partition = key
            ctx = HandlerResultContext(topic=topic, partition=partition)
            tasks.append(
                asyncio.create_task(
                    process_batch(
                        ctx,
                        partition_messages,
                        self.handler_func,
                    )
                )
            )
        # TODO: Allow user controlled semaphore if they have massive (topic, partition) combinations.
        # TODO: On unhandled exceptions, allow user controlled behaviour (Reseek vs Retry/DLQ)?
        topic_partition_results: list[
            HandlerResultContext | BaseException
        ] = await asyncio.gather(*tasks, return_exceptions=True)
        return topic_partition_results

    async def _process_results(
        self, results: list[HandlerResultContext]
    ) -> tuple[list[TopicPartition], list[TopicPartition]]:
        """_process_results parses the user results provided by upto N batched handler functions.  This
        function is responsible for preventing user error and potential message loss.  It has a complex
        decision tree for deriving what is actually committable."""
        successful_partitions = []
        blocked_partitions = []
        poisoned_partitions = {}
        for result in results:
            if isinstance(result, BatchedWrappedUnhandledException):
                # map the exception id back to the task above, we set the task
                # name when fanning out to be the "(topic, partition)".
                # TODO: Not working for now - how do we get the context back here?
                ...
            else:
                # The BatchHandler will include the necessary information to dictate the
                # remaining blocked or poisoned message(s).
                # TODO: successful needs to be aware of blocked to not mark something > than
                # lowest blocked (per partition as successful).
                successful_partitions.extend(
                    [
                        TopicPartition(
                            topic=msg.topic(),
                            partition=msg.partition(),
                            offset=msg.offset(),
                        )
                        for msg in result.succeeded
                    ]
                )
                blocked_partitions.extend(
                    [
                        TopicPartition(
                            topic=msg.topic(),
                            partition=msg.partition(),
                            offset=msg.offset(),
                        )
                        for msg in result.blocked
                    ]
                )

        # poisoned_partitions should be empty at this point (and merged into successful)
        # unless they failed on the enqueueing mechanism, for now, this will cause an
        # exit/crash, but not until we have committed the successful offsets up to that
        # point.  This will be more 'user-configurable' in the future.
        if poisoned_partitions:
            # if the user has poisoned, but not configured a place for them to go, exit.
            if self.retrier is None:
                raise PoisonedMessagesWithNowhereToGoException(
                    "poisoned messages with no retry/dlq configured"
                )
            else:
                # attempt to 'enqueue' the messages to the retry queue
                # if successful, they can be marked 'successful'
                # if failing, how should we handle it?
                # They can be blocked + re-seeked.
                # They can cause an exit.
                # They can be 'ignored' and continue on.
                ...

        # TODO: Generally this needs alot of logic around managing mixed results with blocked/poisoned + success.
        return successful_partitions, blocked_partitions

    async def _store_messages(
        self,
        messages: list[Message] | list[TopicPartition],
    ) -> None:
        """
        _store_messages calculates the highest per (topic, partition) from a grouping of messages
        and stores those offsets in a single call.

        Storing a single value for each partition is sufficient, and the values stored should be the
        next offset to consume (max+1).

        :param messages: The messages to store.  These can either be a confluent kafka Message type
        in which case the topic/partition are callable methods and invoked, or a list of `TopicPartition`
        objects.
        """

        # calculate the highest (max) offset to commit based on each (topic, partition) combination
        # of the input messages.
        offsets_to_commit: dict[tuple[str, int], int] = {}
        for message in messages:
            topic = message.topic() if callable(message.topic) else message.topic
            partition = (
                message.partition()
                if callable(message.partition)
                else message.partition
            )
            offset = message.offset() if callable(message.offset) else message.offset
            key = (topic, partition)
            offsets_to_commit[key] = max(offsets_to_commit.get(key, -1), offset)

        # build a single TopicPartition object for each combination of (topic, partition) - we only
        # need to store the 'highest', not all of them.
        highest_offsets = [
            TopicPartition(topic, partition, offset + 1)
            for (topic, partition), offset in offsets_to_commit.items()
        ]

        await self._store_offsets(offsets=highest_offsets)

    async def _store_offsets(self, offsets: list[TopicPartition]):
        """_store_offsets stores the offsets locally ready for committing in the future."""
        await self.consumer.store_offsets(offsets=offsets)

    async def _commit(self, *, asynchronous: bool) -> None:
        out = await self.consumer.commit(asynchronous=asynchronous)
        failed = [tp.error() for tp in out if tp.error is not None]
        if failed:
            # TODO: Remove later
            raise Exception("some partitions failed")

    async def _on_assign(
        self, _: AIOConsumer, partitions: list[TopicPartition]
    ) -> None:
        """on_assign retrieves the incremental partition updates.  The consumer
        can be multi-topic aware, so we need to keep track of per topic partitions."""
        async with self.rebalance_lock:
            before = self.assigned_partitions.copy()
            for partition in partitions:
                topic, partition = partition.topic, partition.partition
                self.assigned_partitions[topic].add(partition)
            self.consumer_logger.debug(
                "consumer was assigned new partitions: (before=%s), (after=%s)",
                before,
                self.assigned_partitions,
            )
        # TODO: incremental assign if KIP-848
        # await self.consumer.assign(partitions)

    async def _on_revoke(
        self, _: AIOConsumer, partitions: list[TopicPartition]
    ) -> None:
        """_on_revoke is called during a rebalance when this particular consumer has
        lost some of it's previously owned partitions.  It should gracefully commit
        any offsets for these partitions to prevent message duplication etc when
        reassigned to another consumer in the group."""
        async with self.rebalance_lock:
            before = self.assigned_partitions.copy()
            for partition in partitions:
                topic, partition = partition.topic, partition.partition
                self.assigned_partitions[topic].discard(partition)
            self.consumer_logger.debug(
                "consumer had partitions revoked: (before=%s), (after=%s)",
                before,
                self.assigned_partitions,
            )

        # commit anything stored already.
        # try a synchronous commit here, to minimise duplication after rebalances.
        try:
            await self.consumer.commit(asynchronous=False)
        except KafkaException as exc:
            err: KafkaError = exc.args[0]
            if err.code() == "-168":
                # It's possible, rebalances can happen when no messages have actually been stored.
                # when auto store offsets is off.
                pass
        # TODO: KIP-848 incremental unassign?
        # await self.consumer.incremental_unassign(partitions)

    def error_cb(self, err: KafkaError) -> None:
        """error_cb is the default handle for global errors.  Importantly these
        errors are pretty much informative and no real action should need to be
        taken.  If the user does not specify one in their config, this will be
        used instead."""
        self.consumer_logger.error("received transient error: %s", err)

    async def stop(self, wait: bool = True) -> None:
        """stop signals that the consumer should begin a graceful shutdown.
        This will still allow in flight batches to be processed."""
        self.interrupted = True
        if wait:
            while not self.done:
                await asyncio.sleep(0.1)

    async def __aenter__(self) -> typing.Self:
        """__enter__ allows the KafkaConsumer instance to be used as a context
        manager, guaranteeing its graceful exit and teardown."""
        await self.consume()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.consumer.close()
        return None
