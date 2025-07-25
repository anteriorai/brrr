import pytest

import brrr
from brrr import AppWorker, OnlyInBrrrError
from brrr.pickle_codec import PickleCodec
from brrr.backends.in_memory import InMemoryByteStore

from .closable_test_queue import ClosableInMemQueue

TOPIC = "brrr-test"


async def test_only_no_brrr():
    @brrr.handler_no_arg
    @brrr.only
    async def foo(a: int) -> int:
        return a * 2

    with pytest.raises(OnlyInBrrrError):
        await foo(3)


async def test_only_in_brrr():
    store = InMemoryByteStore()
    queue = ClosableInMemQueue([TOPIC])

    @brrr.handler_no_arg
    @brrr.only
    async def foo(a: int) -> int:
        await queue.close()
        return a * 2

    async with brrr.serve(queue, store, store) as conn:
        app = AppWorker(handlers=dict(foo=foo), codec=PickleCodec(), connection=conn)
        await app.schedule(foo, topic=TOPIC)(5)
        await conn.loop(TOPIC, app.handle)
        await queue.join()
        assert await app.read(foo)(5) == 10


async def test_only_in_fake_brrr():
    @brrr.handler_no_arg
    @brrr.only
    async def foo(a: int) -> int:
        return a * 2

    with brrr.allow_only():
        assert await foo(7) == 14
