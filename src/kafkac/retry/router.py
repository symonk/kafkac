from .cfg import RetryConfig


class RetryRouter:
    """RetryRouter is responsible for passing a message on to the appropriate
    retry (or final) dead letter queue based on message meta data.  Kafkac automatically
    persists this meta data across retries and takes 100% control of the retry/DLQ
    process.

    Client code just needs to provide:

        * A producer config (librdkafka)
        * A retry config
        * Ensure the producer has appropriate access/ACL's to write to the configured topics
    """
    def __init__(self,
                 retry_cfg: RetryConfig) -> None:
        self.retry_cfg = retry_cfg


