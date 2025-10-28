import pytest

from kafkac import AsyncKafkaConsumer


def test_missing_group_id_raises_value_error() -> None:
    with pytest.raises(ValueError) as err:
        AsyncKafkaConsumer(
            handler_func=None,
            batch_size=1,
            topic_regexes=("foo",),
            config={},
        )
    assert (
        str(err.value)
        == "consumer must be assigned a `group.id` in the librdkafka config"
    )
