import typing
from confluent_kafka import Message

# FilterFunc defines the signature for user defined filter functions
FilterFunc = typing.Callable[[Message], typing.Awaitable[bool]]