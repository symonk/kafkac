## 🐍 kafkac — A Structured Kafka Consumer Framework for Python

> [!IMPORTANT]
> kafkac prioritises correctness and speed, in that order, avoiding message loss at all costs.

**kafkac** is a minimal, opinionated framework for building reliable Kafka consumers in Python using the [confluent-kafka](https://github.com/confluentinc/confluent-kafka-python) client.
It abstracts away the boilerplate of manual offset handling, shutdown coordination, and message deserialization - giving you a clean async interface for consuming messages safely and predictably.

---

### ⚙️ Core Features

- ⚡️ Fully asynchronous message consumption
- 🧬 Version-aware model deserialization (Pydantic)
- 🛡 Handles common Kafka edge cases and failure scenarios
- 📦 Batch consumption to reduce RTT and executor overhead
- 🧾 Header-level message filtering support
- 📊 Built-in metrics & OpenTelemetry integration
- 🧩 Pluggable middleware for pre/post-processing
- 🪦 Automatic dead-letter queueing for poison-pill messages
- 🔁 Smart retries with exponential backoff
- 🧘 Automatic rebalance management
- ✨ And more...


---

### 🧠 Quick Start

```python
# TODO!
```

---

### Contributing

The project uses `testcontainers` to run an actual `kafka` container throughout integration tests to ensure it
is tested against something that at least resembles the real world.  In order for this to function, ensure the
`docker` service is running.
