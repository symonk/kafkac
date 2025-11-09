## 🐍 kafkac — A Kafka Consumer framework for python

`kafkac` is a batteries-included python kafka consumer, built on top of `librdkafka`.  It aims to simplify
the complexities and edge cases of writing a consumer.  You simply need to plugin some basic `librdkafka`
configurations and implement a `handler` for processing your messages.

---

### ⚙️ Core Features

- ⚡️ Super fast and fully `asynchronous`
- 🧬 Automatic serialisation of messages, version-aware based on message `version` header if set.
- 🛡 Robust error handling for stability.
- 📦 Multi topic, batch consumption.
- 🧾 Message header filtering support with baked in common filters.
- 📊 Event system for useful statistics.
- 🪦 `Deadlettering` support for blocking messages baked in.
- 🔁 Automatic retries with customisable behaviour for different errors.
- 🧘 Automatic rebalance handling, fully supports `KIP-848` (cooperative rebalancing).
- ✨ Much more...

---

### Benchmarks

Below are some benchmarks that preload various levels of messages onto a topic, run a `kafkac` consumer to
process those messages, writing the messages to another topic, confirming all the messages are accounted for.

// TODO

---

### 🧠 Quick Start

```python
import asyncio

from kafkac import AsyncKafkaConsumer
from kafkac import PartitionResult
from confluent_kafka import Message


async def handler(messages: list[Message]) -> PartitionResult:
    return PartitionResult(succeeded=messages)


async def main():
    config = {
        "group.id": "foo",
        "bootstrap.servers": "localhost:9092",
    },
    async with AsyncKafkaConsumer(
            handler_func=handler,
            config=config,
            topic_regexes=["^topic$"],
            batch_size=1000,
    ) as consumer:
        await asyncio.sleep(60)
        await consumer.stop()
        # context manager will exit cleanly once the consumer has finalised.
        # last messages will be processed and handled before graceful exit.


if __name__ == "__main__":
    asyncio.gather(main())

```

---

### Contributing

The project uses `testcontainers` to run an actual `kafka` container throughout integration tests to ensure it
is tested against something that at least resembles the real world.  In order for this to function, ensure the
`docker` service is running.
