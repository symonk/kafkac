class KafkacException(Exception):
    """Base exception, all things raised by kafkac will be a subclass
    of this."""

    def __init__(self, msg: str) -> None:
        super().__init__(msg)


class NoConsumerGroupIdProvidedException(KafkacException):
    """Raised when no `group.id` is provided in the config provided to the `AsyncKafkaConsumer` constructor"""


class InvalidHandlerReturnTypeException(KafkacException):
    """Raised when the return type of a handler coroutine does not return a `BatchResult` object."""


class InvalidHandlerFunctionException(KafkacException):
    """Raised when the client does not provide the appropriate type for processing handler funcs"""


class MismatchHandlerContextResultsException(KafkacException):
    """Raised when the batch handler is sent N number of messages and only stores result data for N-N results"""

class UnsupportedMessagingGroupException(KafkacException):
    """Raised when the messaging group option (task_mode) provided is not supported"""