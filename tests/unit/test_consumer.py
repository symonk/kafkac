import pytest

from kafkac import AsyncKafkaConsumer
from kafkac import InvalidHandlerFunctionException


class NoOpTestHandler:
    async def __call__(self, *args, **kwargs): ...


def test_missing_group_id_raises_value_error() -> None:
    with pytest.raises(ValueError) as err:
        AsyncKafkaConsumer(
            handler_func=NoOpTestHandler(),
            batch_size=1,
            topic_regexes=["foo"],
            config={},
        )
    assert (
        str(err.value)
        == "consumer must be assigned a `group.id` in the librdkafka config"
    )


def test_invalid_handler_func_raises() -> None:
    error_message = (
        "type of handler_func must be `MessageHandlerFunc` or `MessagesHandlerFunc`"
    )
    with pytest.raises(InvalidHandlerFunctionException, match=error_message):
        AsyncKafkaConsumer(
            handler_func=None,
            batch_size=1,
            config={},
            topic_regexes=[],
        )
