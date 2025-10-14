
class KafkacException(Exception):
    """Base exception, all things raised by kafkac will be a subclass
    of this."""
    def __init__(self, msg: str) -> None:
        super().__init__(msg)