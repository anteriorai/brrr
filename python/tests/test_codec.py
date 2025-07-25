from collections import Counter
import dataclasses
import pickle
from unittest.mock import Mock, call

import brrr
from brrr import AppWorker
from brrr.app import ActiveWorker
from brrr.backends.in_memory import InMemoryByteStore
from brrr.pickle_codec import PickleCodec

from .closable_test_queue import ClosableInMemQueue


TOPIC = "test"


async def test_codec_key_no_args():
    calls = Counter()
    store = InMemoryByteStore()
    queue = ClosableInMemQueue([TOPIC])
    codec = PickleCodec()

    old = codec.encode_call

    def encode_call(task_name, args, kwargs):
        call = old(task_name, args, kwargs)
        bare_call = old(task_name, (), {})
        return dataclasses.replace(call, call_hash=bare_call.call_hash)

    codec.encode_call = Mock(side_effect=encode_call)

    @brrr.no_app_arg
    async def same(a: int) -> int:
        assert a == 1
        calls[f"same({a})"] += 1
        return a

    async def foo(app: ActiveWorker, a: int) -> int:
        calls[f"foo({a})"] += 1

        val = 0
        # Call in deterministic order for the test’s sake
        for i in range(1, a + 1):
            val += await app.call(same)(i)

        assert val == a
        await queue.close()
        return val

    async with brrr.serve(queue, store, store) as conn:
        app = AppWorker(handlers=dict(foo=foo, same=same), codec=codec, connection=conn)
        await app.schedule(foo, topic=TOPIC)(50)
        await conn.loop(TOPIC, app.handle)

    await queue.join()
    assert calls == Counter(
        {
            "same(1)": 1,
            "foo(50)": 2,
        }
    )
    codec.encode_call.assert_called()


async def test_codec_determinstic():
    call1 = PickleCodec().encode_call("foo", (1, 2), dict(b=4, a=3))
    call2 = PickleCodec().encode_call("foo", (1, 2), dict(a=3, b=4))
    assert call1.call_hash == call2.call_hash


async def test_codec_api():
    store = InMemoryByteStore()
    queue = ClosableInMemQueue([TOPIC])
    codec = Mock(wraps=PickleCodec())

    @brrr.no_app_arg
    async def plus(x: int, y: str) -> int:
        return x + int(y)

    async def foo(app: ActiveWorker) -> int:
        val = (
            await app.call(plus)(1, "2")
            + await app.call(plus)(x=3, y="4")
            + await app.call(plus)(*(5, "6"))
            + await app.call(plus)(**dict(x=7, y="8"))
        )
        assert val == sum(range(9))
        await queue.close()
        return val

    async with brrr.serve(queue, store, store) as conn:
        app = AppWorker(handlers=dict(foo=foo, plus=plus), codec=codec, connection=conn)
        await app.schedule("foo", topic=TOPIC)()
        await conn.loop(TOPIC, app.handle)

    await queue.join()
    codec.encode_call.assert_has_calls(
        [
            call("foo", (), {}),
            call("plus", (1, "2"), {}),
            call("plus", (), {"x": 3, "y": "4"}),
            call("plus", (5, "6"), {}),
            call("plus", (), {"x": 7, "y": "8"}),
        ],
        any_order=True,
    )

    # The Call argument’s task_name to invoke_task is easiest to test.
    assert Counter(foo=5, plus=4) == Counter(
        map(lambda c: c[0][0].task_name, codec.invoke_task.call_args_list)
    )

    for c in codec.decode_return.call_args_list:
        ret = c[0][0], pickle.loads(c[0][1])
        # I don’t want to hard-code too much of the implementation in the test
        assert ret in {
            ("plus", 3),
            ("plus", 7),
            ("plus", 11),
            ("plus", 15),
            ("foo", sum(range(9))),
        }
