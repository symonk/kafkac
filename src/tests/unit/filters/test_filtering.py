import pytest
from pytest_mock import MockType

from kafkac.filters import filter_contains_header_fn

"""
As of 28/10/2025, confluent-kafka does not allow instantiating `Message` objects.
see: https://github.com/confluentinc/confluent-kafka-python/issues/1535
This tests with mocks, but ideally it should create messages.  Basic filtering
integration tests should also be added.
"""


@pytest.mark.asyncio
async def test_without_headers(mocker: MockType):
    message = mocker.Mock()
    message.headers.return_value = None
    coro = filter_contains_header_fn(message)
    assert not await coro(message)


@pytest.mark.asyncio
async def test_filter_contains_header_fn(mocker: MockType):
    message = mocker.Mock()
    message.headers.return_value = [("foo", b"value")]
    coro = filter_contains_header_fn("foo")
    assert await coro(message)


@pytest.mark.asyncio
async def test_filter_contains_header_fn_invalid(mocker: MockType):
    message = mocker.Mock()
    message.headers.return_value = [("foo", b"value")]
    coro = filter_contains_header_fn("no")
    assert not await coro(message)
