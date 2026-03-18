import pytest
from confluent_kafka import Message
from pytest_mock import MockerFixture

from kafkac.filters import FilterFuncs
from kafkac.filters import filter_contains_header_fn

# TODO: Stop using mocks for Message objects, this is fixed in a recent upgraded version.

async def always_false(message: Message) -> bool:
    return False


async def always_true(message: Message) -> bool:
    return True


@pytest.mark.asyncio
async def test_empty_filter_funcs_raises() -> None:
    with pytest.raises(ValueError, match="cannot use FilterFuncs without filter functions"):
        FilterFuncs(topics={"foo"}, funcs=[])

@pytest.mark.asyncio
async def test_filtering_applies_to_topics_correctly() -> None:
    message = Message(topic="foo", offset=0, partition=0, key=b"bar")
    funcs =  FilterFuncs(topics={"bar"}, funcs=[always_false])
    filtered = await funcs.discard([message])
    assert len(filtered) == 0

@pytest.mark.asyncio
async def test_filtering_applies_to_topics_correctly_with_filters() -> None:
    message = Message(topic="foo", offset=0, partition=0, key=b"bar")
    funcs = FilterFuncs(topics={"foo"}, funcs=[always_true])
    filtered = await funcs.discard([message])
    assert len(filtered) == 1

@pytest.mark.asyncio
async def test_filtering_only_applies_to_topics_when_func_returns_true() -> None:
    message = Message(topic="foo", offset=0, partition=0, key=b"bar")
    funcs = FilterFuncs(topics={"foo"}, funcs=[always_false, always_false, always_false, always_true])
    filtered = await funcs.discard([message])
    assert len(filtered) == 1

@pytest.mark.asyncio
async def test_without_headers(mocker: MockerFixture) -> None:
    message = mocker.Mock()
    message.headers.return_value = None
    coro = filter_contains_header_fn(message)
    assert not await coro(message)
    message.headers.assert_called_once()


@pytest.mark.asyncio
async def test_filter_contains_header_fn(mocker: MockerFixture):
    message = mocker.Mock()
    message.headers.return_value = [("foo", b"value")]
    coro = filter_contains_header_fn("foo")
    assert await coro(message)
    message.headers.assert_called_once()


@pytest.mark.asyncio
async def test_filter_contains_header_fn_invalid(mocker: MockerFixture):
    message = mocker.Mock()
    message.headers.return_value = [("foo", b"value")]
    coro = filter_contains_header_fn("no")
    assert not await coro(message)
    message.headers.assert_called_once()
