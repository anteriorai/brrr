from collections import Counter
import dataclasses
import pickle
from unittest.mock import Mock, call

from brrr import Brrr
from brrr.backends.in_memory import InMemoryByteStore
from brrr.naive_codec import PickleCodec

from .closable_test_queue import ClosableInMemQueue


async def test_codec_key_no_args():
    b = Brrr()
    calls = Counter()
    store = InMemoryByteStore()
    queue = ClosableInMemQueue()
    codec = PickleCodec()

    old = codec.create_call

    def create_call(task_name, args, kwargs):
        call = old(task_name, args, kwargs)
        bare_call = old(task_name, (), {})
        return dataclasses.replace(call, memo_key=bare_call.memo_key)

    codec.create_call = Mock(side_effect=create_call)

    @b.task
    async def same(a: int) -> int:
        calls[f"same({a})"] += 1
        return a

    @b.task
    async def foo(a: int) -> int:
        calls[f"foo({a})"] += 1

        val = 0
        # Call in deterministic order for the test’s sake
        for i in range(1, a + 1):
            val += await same(i)

        assert val == a
        await queue.close()
        return val

    b.setup(queue, store, store, codec)
    await b.schedule("test", "foo", (50,), {})
    await b.wrrrk("test")
    await queue.join()
    assert calls == Counter(
        {
            "same(1)": 1,
            "foo(50)": 2,
        }
    )
    codec.create_call.assert_called()


async def test_codec_determinstic():
    call1 = PickleCodec().create_call("foo", (1, 2), dict(b=4, a=3))
    call2 = PickleCodec().create_call("foo", (1, 2), dict(a=3, b=4))
    assert call1.memo_key == call2.memo_key


async def test_codec_api():
    b = Brrr()
    store = InMemoryByteStore()
    queue = ClosableInMemQueue()
    codec = Mock(wraps=PickleCodec())

    @b.task
    async def plus(x: int, y: str) -> int:
        return x + int(y)

    @b.task
    async def foo() -> int:
        val = (
            await plus(1, "2")
            + await plus(x=3, y="4")
            + await plus(*(5, "6"))
            + await plus(**dict(x=7, y="8"))
        )
        assert val == sum(range(9))
        await queue.close()
        return val

    b.setup(queue, store, store, codec)
    await b.schedule("test", "foo", (), {})
    await b.wrrrk("test")
    await queue.join()
    codec.create_call.assert_has_calls(
        [
            call("foo", (), {}),
            call("plus", (1, "2"), {}),
            call("plus", (), {"x": 3, "y": "4"}),
            call("plus", (5, "6"), {}),
            call("plus", (), {"x": 7, "y": "8"}),
        ],
        any_order=True,
    )
    assert codec.encode_call.call_count == 5

    # The “name” argument to invoke_task is easiest to test.
    assert Counter(foo=5, plus=4) == Counter(
        map(lambda c: c[0][1], codec.invoke_task.call_args_list)
    )

    for c in codec.decode_return.call_args_list:
        ret = pickle.loads(c[0][0])
        # I don’t want to hard-code too much of the implementation in the test
        assert ret in (3, 7, 11, 15, sum(range(9)))
