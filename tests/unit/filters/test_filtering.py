from confluent_kafka import Message

# TODO: Stop using mocks for Message objects, this is fixed in a recent upgraded version.


async def always_false(message: Message) -> bool:
    return False


async def always_true(message: Message) -> bool:
    return True
