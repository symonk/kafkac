> [!CAUTION]
> Kafkac is NOT production ready, do not use it as such yet.

> [!IMPORTANT]
> `Kafkac` only supports kafka broker version `4.0.0+`

## 🐍 kafkac — A Kafka Consumer framework for python

`kafkac` is a batteries-included python kafka consumer, built on top of `librdkafka`.  It aims to simplify
the complexities and edge cases of writing a consumer.  You simply need to plugin some basic `librdkafka`
configurations and implement a `handler` for processing your messages.

---

### ⚙️ Core Features

- ⚡️ Super fast and fully `asynchronous`.
- 🧬 Automatic serialisation of messages, version-aware based on message `version` header if set.
- 🛡 Robust error handling for stability.
- 📦 Multi topic, batch consumption.
- 🧾 Message header filtering support with baked in common filters.
- 📊 Event system for useful statistics.
- 🪦 `Deadlettering/Retry Queue` support for blocking messages baked in.
- 🔁 Automatic retries with customisable behaviour for different errors.
- 🧘 Automatic rebalance handling, fully supports `KIP-848` (cooperative rebalancing).
- ✨ Much more...

---

### 🧠 Quick Start

In terms of usability, implementing your own consumer is straight forward, you should:

    1. Build your appropriate `librdkafka` configuration for the consumer.
    2. Decide how you would like your messages delivered your handler (ProcessingOpts).
    3. Implement an async handler for those messages.
    4. Decide how ordering should (or shouldn't be managed)
    5. Populate success/failure of messages within the handler injected context.
    6. Decide (Optionally) if you would like retry queue/DLQ and provide a `RetryConfig` for those.
    7. Kafkac will handle the rest, taking care of all the semantics and edge cases.


```python
import asyncio

from kafkac import AsyncKafkaConsumer
from kafkac import HandlerResultContext
from confluent_kafka import Message


async def handler(messages: list[Message]) -> HandlerResultContext:
    return HandlerResultContext(succeeded=messages)


async def main():
    print("implement later")


if __name__ == "__main__":
    asyncio.run(main())

```

---

### Developer Guide

This section includes the 'must knows' when interacting with the `kafkac` library.

#### Access debug logs:

If you have a need to debug the consumer and access the underlying `librdkafka` debug logs, this can be achieved
by providing either a coma separated string to `debug="...,..."` when instantiating the async consumer.  The supported options
for this are: `cgrp,topic,fetch,consumer`.  Alternatively, if `KAFKA_CONFIG` contains a comma separated string of some or more
of these values, kafkac will parse it.  Priority is given to the specified `debug=""` string if provided.

Providing a `logger` object when instantiating the async consumer, will cause these
debug logs to be routed to your handler and you can do with them what you see fit.

---

### Benchmarks

Below are some benchmarks that preload various levels of messages onto a topic, run a `kafkac` consumer to
process those messages, writing the messages to another topic, confirming all the messages are accounted for.

// TODO

---

### Contributing

The project uses `testcontainers` to run an actual `kafka` container throughout integration tests to ensure it
is tested against something that at least resembles the real world.  In order for this to function, ensure the
`docker` service is running.
