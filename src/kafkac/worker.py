from confluent_kafka import Message

from .handler import MessagesHandlerFunc
from .result import KafkacContext


class BatchedWrappedUnhandledException(Exception):
    """WrappedException retains the topic and partition of the messages sent to
    the batched handler when user code fails and an unhandled exception leaks
    out of the handler."""

    def __init__(self, topic: str, partition: int, exc: Exception) -> None:
        super().__init__(f"Handler failed for {topic}:{partition}: {exc}")
        self.topic = topic
        self.partition = partition

    @property
    def cause(self) -> BaseException | None:
        return self.__cause__


# TODO: Improve/implement
async def process_batch(
    context: KafkacContext,
    messages: list[Message],
    handler: MessagesHandlerFunc,
) -> KafkacContext:
    """processor is responsible for processing messages received by the consumer
    for individual partitions."""
    try:
        return await handler(context, messages)
    except Exception as exc:
        raise BatchedWrappedUnhandledException(topic=context.topic, exc=exc) from exc
