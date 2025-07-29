from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
import functools
from typing import Awaitable, Callable


_in_brrr: ContextVar[bool] = ContextVar("brrr.only", default=False)


class OnlyInBrrrError(Exception):
    pass


@contextmanager
def with_context[T](var: ContextVar[T], val: T) -> Iterator[None]:
    token = var.set(val)
    try:
        yield
    finally:
        var.reset(token)


def only[**P, R](f: Callable[P, Awaitable[R]]) -> Callable[P, Awaitable[R]]:
    @functools.wraps(f)
    async def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
        if not _in_brrr.get():
            raise OnlyInBrrrError()
        return await f(*args, **kwargs)

    return wrapper


@contextmanager
def allow_only() -> Iterator[None]:
    with with_context(_in_brrr, True):
        yield
