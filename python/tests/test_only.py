from brrr.local_app import local_app
import pytest

import brrr
from brrr import OnlyInBrrrError
from brrr.pickle_codec import PickleCodec

TOPIC = "brrr-test"


async def test_only_no_brrr():
    @brrr.handler_no_arg
    @brrr.only
    async def foo(a: int) -> int:
        return a * 2

    with pytest.raises(OnlyInBrrrError):
        await foo(3)


async def test_only_in_brrr():
    @brrr.handler_no_arg
    @brrr.only
    async def foo(a: int) -> int:
        return a * 2

    async with local_app(
        topic=TOPIC, handlers=dict(foo=foo), codec=PickleCodec()
    ) as app:
        await app.schedule(foo)(5)
        await app.run()
        assert await app.read(foo)(5) == 10


async def test_only_in_fake_brrr():
    @brrr.handler_no_arg
    @brrr.only
    async def foo(a: int) -> int:
        return a * 2

    with brrr.allow_only():
        assert await foo(7) == 14
