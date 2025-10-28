import asyncio
import json
import logging
import typing
from collections import defaultdict
from pprint import pformat

from confluent_kafka import KafkaError
from confluent_kafka import Message
from confluent_kafka import TopicPartition
from confluent_kafka.experimental.aio import AIOConsumer

from kafkac.filters.filter import FilterFunc

from .exception import InvalidHandlerReturnTypeException
from .exception import NoConsumerGroupIdProvidedException
from .handler import BatchResult
from .handler import HandlerFunc

# add a non-intrusive logger, allowing clients to view some useful information
# but not getting in their way.
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


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

    TODO: Document latency vs throughput scenarios and tuning.
    """

    def __init__(
        self,
        *,
        handler_func: HandlerFunc,
        config: dict[str, typing.Any],
        batch_size: int,
        topic_regexes: list[str],
        poll_interval: float = 0.1,
        filter_func: FilterFunc | None = None,
        dlq_topic: str | None = None,
        batch_timeout: float = 60.0,  # TODO: Should probably be None if not specified.
    ) -> None:
        # group.id is a required parameter
        if "group.id" not in config:
            raise NoConsumerGroupIdProvidedException(
                "consumer must be assigned a `group.id` in the librdkafka config"
            )
        # ensure a positive batch size, while also keeping it below the librdkafka limit of
        # 1M messages, if higher than this the core library will raise an error on consume(...)
        self.batch_size = min(max(batch_size, 1), 1_000_000)
        self.handler_func = handler_func
        self.consumer: AIOConsumer | None = None
        self.running = False
        self.interrupted = False
        self.topics_regexes = topic_regexes
        self.librdkafka_config = self._prepare_cfg(config)
        self.poll_interval = max(0.1, poll_interval)
        self.filter_func = filter_func
        # an (optional) dead letter queue topic.  For now this only supports the same cluster
        # but will widen substantially in the future.
        self.dlq_func = dlq_topic
        # track `done` which signals after interruption, the finalizers are complete and it is safe
        # to fully close out the consumer.
        self.done = False
        self.workers = 8  # TODO: Derive this, or make it available to users.
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

    def _prepare_cfg(
        self,
        user_cfg: dict[str, typing.Any],
    ) -> dict[str, typing.Any]:
        """
        TODO: Implement, we should probably enforce auto commit offset off, and maybe some others.
        :return:
        """
        user_cfg["enable.partition.eof"] = False
        user_cfg["enable.auto.commit"] = False
        user_cfg["enable.auto.offset.store"] = False
        user_cfg.setdefault("stats_cb", stats_cb)
        # TODO: only enforce this if supporting a modern enough broker setup.
        user_cfg["group.remote.assignor"] = "cooperative-sticky"
        user_cfg["group.consumer.session.timeout.ms"] = 45000
        user_cfg["group.protocol"] = "consumer"
        user_cfg.setdefault("error_cb", self.error_cb)
        user_cfg.setdefault("throttle_cb", throttle_cb)
        return user_cfg

    async def start(self) -> None:
        """start signals the consumer to actually begin.  This is implicit
        when KafkaConsumer is used as a context manager."""
        try:
            self.consumer = AIOConsumer(
                consumer_conf=self.librdkafka_config, max_workers=self.workers
            )
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
                    num_messages=self.batch_size, timeout=self.poll_interval
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
                filtered_messages = (
                    await self._handle_filters(messages)
                    if self.filter_func is not None
                    else messages
                )
                if len(filtered_messages) == 0:
                    # the entire batch was 'filtered' out by the user.
                    # TODO: handle exceptions (RuntimeError/KafkaException)
                    await self._commit(
                        offsets=get_highest_offset_per_partition(filtered_messages),
                        block=True,
                    )
                    continue

                # for each of the partitions this consumed is assigned, fan out the partitions messages
                # as one transactional unit of work.  Should any message fail throughout processing the
                # entire batch is abandoned and the partition will be considered blocked, not stored and
                # will try again on the next consume loop.
                batch_result: BatchResult = await self.handler_func(filtered_messages)
                if not isinstance(batch_result, BatchResult):
                    raise InvalidHandlerReturnTypeException(
                        "handler coroutines must return a `BatchResult`"
                    )

                # every partition is blocked in a transient way, next loop the same batch of messages
                # will be retried, unless the batch was not full in which case a superset of this batch
                # will be retried.
                if batch_result.all_transient:
                    continue

                # inspect the batch results to derive offsets to store and commit or dead letter behaviour
                # to manage.
                # check if the entire batch was successful and commit all
                if batch_result.all_success:
                    await self._commit(
                        offsets=get_highest_offset_per_partition(filtered_messages),
                        block=True,
                    )
                    continue

                # check if there was dead letter fatal failures, for now hard coded printing them
                # dead letters are technically 'successful' and from the consumers point of view
                # should roll the offset forward.
                if batch_result.dead_letter:
                    logger.debug("full batch of dead letter messages")
                    await self._commit(
                        offsets=get_highest_offset_per_partition(filtered_messages),
                        block=True,
                    )

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

        results = await self.consumer.commit(**commit_kw)

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

    async def _on_assign(
        self, _: AIOConsumer, partitions: list[TopicPartition]
    ) -> None:
        """on_assign retrieves the incremental partition updates.  The consumer
        can be multi-topic aware, so we need to keep track of per topic partitions."""
        async with self.rebalance_lock:
            for partition in partitions:
                topic, partition = partition.topic, partition.partition
                self.assigned_partitions[topic].add(partition)
        await self.consumer.incremental_assign(partitions)

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
        await self.consumer.commit(asynchronous=False)
        await self.consumer.incremental_unassign(partitions)

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

    async def __aenter__(self) -> typing.Self:
        """__enter__ allows the KafkaConsumer instance to be used as a context
        manager, guaranteeing its graceful exit and teardown."""
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.consumer.close()
        return None


async def stats_cb(json_str: str) -> None:
    data = pformat(json.loads(json_str))
    logger.debug(f"received stats: {data}")


async def throttle_cb() -> None:
    logger.debug("throttle_cb")


# TODO: Move to somewhere else later
def get_highest_offset_per_partition(messages: list[Message]) -> list[TopicPartition]:
    tp = {}
    for message in messages:
        if message.partition in tp:
            if message.offset > tp[message.offset()]:
                tp[message.partition].append(
                    TopicPartition(
                        topic=message.topic(),
                        partition=message.partition(),
                        offset=message.offset(),
                    )
                )
        else:
            tp[message.partition] = TopicPartition(
                topic=message.topic(),
                partition=message.partition(),
                offset=message.offset(),
            )

    return list(tp.values())
