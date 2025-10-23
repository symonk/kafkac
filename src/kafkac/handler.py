import typing

class HandlerResult(typing.TypedDict):
    ...

HandlerFunc = typing.Callable[..., HandlerResult]