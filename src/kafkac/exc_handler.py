from dataclasses import dataclass


@dataclass(frozen=True)
class BatchExcHandler:
    """BatchRetry denotes what to do if a batch handler function raises an unhandled
    exception."""

    retries: int = 3
    on: tuple[Exception] = (Exception,)
