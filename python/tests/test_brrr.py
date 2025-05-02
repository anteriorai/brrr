import asyncio
import typing
from collections import Counter

import pytest

from brrr import Brrr
from brrr.backends.in_memory import InMemoryByteStore
from brrr.naive_codec import PickleCodec

from .closable_test_queue import ClosableInMemQueue

TOPIC = "brrr-test"


@pytest.fixture
def handle_nobrrr():
    b = Brrr()

    @b.task
    async def handle_nobrrr(a: int) -> int:
        return a if a == 0 else a + await handle_nobrrr(a - 1)

    return handle_nobrrr


async def test_no_brrr_funcall(handle_nobrrr):
    assert await handle_nobrrr(3) == 6


async def test_no_brrr_map(handle_nobrrr):
    assert await handle_nobrrr.map([[3], [4]]) == [6, 10]


async def test_gather() -> None:
    b = Brrr()

    @b.task
    async def foo(a: int) -> int:
        return a * 2

    @b.task
    async def bar(a: int) -> str:
        return str(a - 1)

    x, y = await b.gather(foo(3), bar(4))
    typing.assert_type(x, int)
    typing.assert_type(y, str)
    assert x, y == (6, "3")


async def _call_nested_gather(*, use_brrr_gather: bool) -> list[str]:
    """
    Helper function to test that brrr.gather runs all brrr tasks in parallel,
    in contrast with how asyncio.gather only runs one at a time.
    """
    b = Brrr()
    calls = []
    store = InMemoryByteStore()
    queue = ClosableInMemQueue()

    @b.task
    async def foo(a: int) -> int:
        calls.append(f"foo({a})")
        return a * 2

    @b.task
    async def bar(a: int) -> int:
        calls.append(f"bar({a})")
        return a - 1

    async def baz(a: int) -> int:
        b = await foo(a)
        return await bar(b)

    @b.task
    async def top(xs: list[int]) -> list[int]:
        calls.append(f"top({xs})")
        if use_brrr_gather:
            result = await b.gather(*[baz(x) for x in xs])
        else:
            result = await asyncio.gather(*[baz(x) for x in xs])
        typing.assert_type(result, list[int])
        # with b.gather, `top` is called twice after its dependencies are done,
        # but we can only close the queue once
        if not queue.closing:
            await queue.close()
        return result

    b.setup(queue, store, store, PickleCodec())
    await b.schedule(TOPIC, "top", ([3, 4],), {})
    await b.wrrrk(TOPIC)
    await queue.join()
    return calls


async def test_brrr_gather():
    """
    Since brrr.gather waits for all Defers to be raised, top should Defer at most twice,
    and both foo calls should happen before both bar calls.

    Example order of events:
    - enqueue top([3, 4])
    - run top([3, 4])
        - attempt foo(3), Defer and enqueue
        - attempt foo(4), Defer and enqueue
        - Defer and enqueue
    - run foo(3)
    - run foo(4)
    - run top([3, 4])
        - attempt baz(3), Defer and enqueue
        - attempt baz(4), Defer and enqueue
        - Defer and enqueue
    - run baz(3)
    - run baz(4)
    - run top([3, 4])
    """
    brrr_calls = await _call_nested_gather(use_brrr_gather=True)
    # TODO: once debouncing is fixed, this should be 3 instead of 5;
    # see test_no_debounce_parent
    assert len([c for c in brrr_calls if c.startswith("top")]) == 5
    foo3, foo4, bar6, bar8 = (
        brrr_calls.index("foo(3)"),
        brrr_calls.index("foo(4)"),
        brrr_calls.index("bar(6)"),
        brrr_calls.index("bar(8)"),
    )
    assert foo3 < bar6
    assert foo3 < bar8
    assert foo4 < bar6
    assert foo4 < bar8


async def test_topics():
    b1 = Brrr()
    b2 = Brrr()
    store = InMemoryByteStore()
    queue = ClosableInMemQueue()

    @b1.task
    async def one(a: int) -> int:
        return a + 5

    @b2.task
    async def two(a: int):
        # N.B.: b2 can use its own brrr instance
        result = await b2.call("t1", "one", (a + 3,), {})
        assert result == 15
        await queue.close()

    b1.setup(queue, store, store, PickleCodec())
    b2.setup(queue, store, store, PickleCodec())
    await b2.schedule("t2", "two", (7,), {})
    await asyncio.gather(b1.wrrrk("t1"), b2.wrrrk("t2"))
    await queue.join()


async def test_asyncio_gather():
    """
    Since asyncio.gather raises the first Defer, top should Defer four times.
    Each foo call should happen before its logical next bar call, but there is no
    guarantee that either foo call happens before the other bar call.
    """
    asyncio_calls = await _call_nested_gather(use_brrr_gather=False)
    assert len([c for c in asyncio_calls if c.startswith("top")]) == 5
    assert asyncio_calls.index("foo(3)") < asyncio_calls.index("bar(6)")
    assert asyncio_calls.index("foo(4)") < asyncio_calls.index("bar(8)")


async def test_nop_closed_queue():
    b = Brrr()
    store = InMemoryByteStore()
    queue = ClosableInMemQueue()
    await queue.close()
    b.setup(queue, store, store, PickleCodec())
    await b.wrrrk(TOPIC)
    await b.wrrrk(TOPIC)
    await b.wrrrk(TOPIC)


async def test_stop_when_empty():
    # Keeping state of the calls to see how often it’s called
    b = Brrr()
    calls_pre = Counter()
    calls_post = Counter()
    store = InMemoryByteStore()
    queue = ClosableInMemQueue()

    @b.task
    async def foo(a: int) -> int:
        calls_pre[a] += 1
        if a == 0:
            return 0
        res = await foo(a - 1)
        calls_post[a] += 1
        if a == 3:
            await queue.close()
        return res

    b.setup(queue, store, store, PickleCodec())
    await b.schedule(TOPIC, "foo", (3,), {})
    await b.wrrrk(TOPIC)
    await queue.join()
    assert calls_pre == Counter({0: 1, 1: 2, 2: 2, 3: 2})
    assert calls_post == Counter({1: 1, 2: 1, 3: 1})


async def test_debounce_child():
    b = Brrr()
    calls = Counter()
    store = InMemoryByteStore()
    queue = ClosableInMemQueue()

    @b.task
    async def foo(a: int) -> int:
        calls[a] += 1
        if a == 0:
            return a

        ret = sum(await foo.map([[a - 1]] * 50))
        if a == 3:
            await queue.close()
        return ret

    b.setup(queue, store, store, PickleCodec())
    await b.schedule(TOPIC, "foo", (3,), {})
    await b.wrrrk(TOPIC)
    await queue.join()
    assert calls == Counter({0: 1, 1: 2, 2: 2, 3: 2})


# This formalizes an anti-feature: we actually do want to debounce calls to the
# same parent.  Let’s at least be explicit about this for now.
async def test_no_debounce_parent():
    b = Brrr()
    calls = Counter()
    store = InMemoryByteStore()
    queue = ClosableInMemQueue()

    @b.task
    async def one(_: int) -> int:
        calls["one"] += 1
        return 1

    @b.task
    async def foo(a: int) -> int:
        calls["foo"] += 1
        # Different argument to avoid debouncing children
        ret = sum(await one.map([[i] for i in range(a)]))
        # Obviously we only actually ever want to reach this point once
        if calls["foo"] == 1 + a:
            await queue.close()
        return ret

    b.setup(queue, store, store, PickleCodec())
    await b.schedule(TOPIC, "foo", (50,), {})
    await b.wrrrk(TOPIC)
    await queue.join()
    # We want foo=2 here
    assert calls == Counter(one=50, foo=51)


async def test_wrrrk_recoverable():
    b = Brrr()
    queue = ClosableInMemQueue()
    store = InMemoryByteStore()
    calls = Counter()

    class MyError(Exception):
        pass

    @b.task
    async def foo(a: int) -> int:
        calls[f"foo({a})"] += 1
        if a == 0:
            raise MyError()
        return await foo(a - 1)

    @b.task
    async def bar(a: int) -> int:
        calls[f"bar({a})"] += 1
        if a == 0:
            return 0
        ret = await bar(a - 1)
        if a == 2:
            await queue.close()
        return ret

    b.setup(queue, store, store, PickleCodec())
    my_error_encountered = False
    await b.schedule(TOPIC, "foo", (2,), {})
    try:
        await b.wrrrk(TOPIC)
    except MyError:
        my_error_encountered = True
    assert my_error_encountered

    # Trick the test queue implementation to survive this
    queue.received[TOPIC] = asyncio.Queue()
    await b.schedule(TOPIC, "bar", (2,), {})
    await b.wrrrk(TOPIC)
    await queue.join()

    assert calls == Counter(
        {
            "foo(0)": 1,
            "foo(1)": 1,
            "foo(2)": 1,
            "bar(0)": 1,
            "bar(1)": 2,
            "bar(2)": 2,
        }
    )


def test_error_on_setup():
    b = Brrr()

    @b.task
    async def foo(a: int):
        return a

    @b.task
    async def foo(a: int):  # noqa: F811
        return a * a

    with pytest.raises(Exception):
        b.setup()
