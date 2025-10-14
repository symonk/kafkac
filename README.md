# kafkac

`kafka` is a simple kafka consumer, wrapping around `confluent-kafka` that handles a lot of the edge cases
for you.  Out of the box features will be:

* Asynchronous API (Fan out processing for batches)
* Automatic version-aware model deserialization
* Error handling
* Metrics / OpenTelemetry capabilities
* Ability to discard messages with simple functions
* Custom user-defined middlewares
* Automatic dead letter queueing for poison-pill messages
* Smart retrying with backoff strategies
* Optional batch processing support
* Optional schema validation
* Kafka metadata/context injection into handlers
* Message key-based routing support
* Graceful shutdown and rebalance-safe processing
* Test harness and dry-run mode for handler logic
* Header parsing utilities
* Configurable offset committing strategies
