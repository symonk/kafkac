> [!CAUTION]
> Kafkac is NOT production ready, do not use it as such yet.

## 🐍 kafkac — A Kafka Consumer framework for python

`kafkac` is a batteries-included python kafka consumer, built on top of `librdkafka`.  It aims to simplify
the complexities and edge cases of writing a consumer.  You simply need to plug in some basic `librdkafka`
configurations and implement a `handler` for processing your messages.

---

### ⚙️ Core Features

- ⚡️ Async API
- 🧬 Automatic Pydantic serialisation supported based on `version` headers
- 🛡 Robust error handling for stability.
- 📦 Multi topic, batch consumption.
- 🧾 Message header filtering support with baked in common filters.
- 🪦 `next-hop` support for retry queue/dlq enqueuing for transient failures.
- 🔁 Supports `head-of-queue` blocking (if desirable)
- ✨ More...

---

### 🧠 Quick Start

In terms of usability, implementing your own consumer is straight forward, you should:

* Build your appropriate `librdkafka` configuration for the consumer.
* Decide how you would like your messages delivered your handler (ProcessingOpts).
* Implement an async handler for those messages.
* Decide how ordering should (or shouldn't be managed)
* Populate success/failure of messages within the handler injected context.
* Decide (Optionally) if you would like retry queue/DLQ and provide a `RetryConfig` for those.
* Kafkac will handle the rest, taking care of all the semantics and edge cases.


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

#### Important Nuances of Head-of-queue blocking

`Kafkac` offers the ability to requeue transient failures to a 'next-hop' queue (or store), this should be the
preferred design methodology, however should you wish to avail of head of queue blocking on transient failures
ensure that you are not fetching sizable batches AND fanning out concurrently on a per message basis inside your
handler function because if you do, you will create potentially manage duplicates while that partition is blocked.

#### Access debug logs:

If you have a need to debug the consumer and access the underlying `librdkafka` debug logs, this can be achieved
by providing either a coma separated string to `debug="...,..."` when instantiating the async consumer.  The supported options
for this are: `cgrp,topic,fetch,consumer`.  Alternatively, if `KAFKA_CONFIG` contains a comma separated string of some or more
of these values, kafkac will parse it.  Priority is given to the specified `debug=""` string if provided.

Providing a `logger` object when instantiating the async consumer, will cause these
debug logs to be routed to your handler and you can do with them what you see fit.

---

### Contributing

The project uses `testcontainers` to run an actual `kafka` container throughout integration tests to ensure it
is tested against something that at least resembles the real world.  In order for this to function, ensure the
`docker` service is running.
