import tenacity
from confluent_kafka import Message
from tenacity import AsyncRetrying

from .exc_handler import BatchRetrier
from .handler import MessagesHandlerFunc
from .result import KafkacContext


class BatchedWrappedUnhandledException(Exception):
    """WrappedException retains the topic and partition of the messages sent to
    the batched handler when user code fails and an unhandled exception leaks
    out of the handler."""

    def __init__(self, messages: list[Message], exc: Exception) -> None:
        super().__init__(f"batch failed because: {exc}")
        self.messages = messages

    @property
    def cause(self) -> BaseException | None:
        return self.__cause__


# TODO: This is running indefinitely on fail.
async def batch_coro(
    context: KafkacContext,
    messages: list[Message],
    handler: MessagesHandlerFunc,
    retries: BatchRetrier,
) -> KafkacContext:
    """processor is responsible for processing messages received by the consumer
    for individual partitions."""
    try:
        async for attempt in AsyncRetrying(
            reraise=True, stop=tenacity.stop_after_attempt(3)
        ):
            with attempt:
                return await handler(context, messages)
    except Exception as exc:
        raise BatchedWrappedUnhandledException(messages, exc) from exc
