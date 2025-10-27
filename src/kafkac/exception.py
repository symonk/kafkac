class KafkacException(Exception):
    """Base exception, all things raised by kafkac will be a subclass
    of this."""

    def __init__(self, msg: str) -> None:
        super().__init__(msg)


class NoConsumerGroupIdProvidedException(KafkacException):
    """Raised when no `group.id` is provided in the config provided to the `AsyncKafkaConsumer` constructor"""


class InvalidHandlerReturnTypeException(KafkacException):
    """Raised when the return type of a handler coroutine does not return a `BatchResult` object."""
