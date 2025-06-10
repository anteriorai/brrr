import asyncio
import typing
from collections import Counter

import pytest

from brrr import Brrr
from brrr.backends.in_memory import InMemoryByteStore
from brrr.pickle_codec import PickleCodec

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
    queue = ClosableInMemQueue([TOPIC])

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
    async with b.wrrrk() as c:
        await c.loop(TOPIC)
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
    queue = ClosableInMemQueue(["t1", "t2"])

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
    async with b1.wrrrk() as c1:
        async with b2.wrrrk() as c2:
            await asyncio.gather(c1.loop("t1"), c2.loop("t2"))
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
    queue = ClosableInMemQueue([TOPIC])
    await queue.close()
    b.setup(queue, store, store, PickleCodec())
    async with b.wrrrk() as c:
        await c.loop(TOPIC)
        await c.loop(TOPIC)
        await c.loop(TOPIC)


async def test_stop_when_empty():
    # Keeping state of the calls to see how often it’s called
    b = Brrr()
    calls_pre = Counter()
    calls_post = Counter()
    store = InMemoryByteStore()
    queue = ClosableInMemQueue([TOPIC])

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
    async with b.wrrrk() as c:
        await c.loop(TOPIC)
    await queue.join()
    assert calls_pre == Counter({0: 1, 1: 2, 2: 2, 3: 2})
    assert calls_post == Counter({1: 1, 2: 1, 3: 1})


async def test_wrrrk_resumable():
    b = Brrr()
    store = InMemoryByteStore()
    queue = ClosableInMemQueue([TOPIC])

    errors = 5

    class MyError(Exception):
        pass

    @b.task
    async def foo(a: int) -> int:
        nonlocal errors
        if errors:
            errors -= 1
            raise MyError("retry")
        await queue.close()
        return a

    b.setup(queue, store, store, PickleCodec())
    while True:
        try:
            await b.schedule(TOPIC, "foo", (3,), {})
            async with b.wrrrk() as c:
                await c.loop(TOPIC)
            break
        except MyError:
            continue

    await queue.join()
    assert errors == 0


@pytest.mark.parametrize("use_gather", [(False,)])
async def test_wrrrk_parallel(use_gather: bool):
    b = Brrr()
    store = InMemoryByteStore()
    queue = ClosableInMemQueue([TOPIC])

    parallel = 5
    barrier: asyncio.Barrier | None = asyncio.Barrier(parallel)

    top_calls = 0

    @b.task
    async def block(a: int) -> int:
        nonlocal barrier
        if barrier is not None:
            await barrier.wait()
        # The barrier was breached once: that is enough to prove _this_ test to
        # be correct.  The tasks end up being run and re-run a few times, and
        # with caching etc it can get confusing to nail the exact amount of
        # parallel runs.  But that’s not what this is testing, this is just
        # testing: if you start N parallel workers, will they all independently
        # handle a job in parallel.  Reaching this line of code proves that.
        # Now it’s done.
        barrier = None
        return a

    @b.task
    async def top() -> None:
        if use_gather:
            await b.gather(*(block(x) for x in range(parallel)))
        else:
            await block.map([[(x,), {}] for x in range(parallel)])

        # Mega hack workaround for our lack of parent debouncing, which causes
        # this to be called multiple times, all of which goes through the queue
        # we’re trying to close.  This if guard guarantees that the queue is
        # only closed on the _last_ call to ‘top’, and we know no other message
        # are put on the queue after this.  Of course the real solution is to
        # debounce calls to the parent!
        nonlocal top_calls
        top_calls += 1
        if top_calls == parallel:
            await queue.close()

    b.setup(queue, store, store, PickleCodec())
    await b.schedule(TOPIC, "top", (), {})
    async with b.wrrrk() as c:
        await asyncio.gather(*(c.loop(TOPIC) for _ in range(parallel)))
    await queue.join()


async def test_stress_parallel():
    b = Brrr()
    store = InMemoryByteStore()
    queue = ClosableInMemQueue([TOPIC])

    @b.task
    async def fib(a: int) -> int:
        if a < 2:
            return a
        return sum(await b.gather(fib(a - 1), fib(a - 2)))

    @b.task
    async def top() -> None:
        n = await fib(1000)
        assert (
            n
            == 43466557686937456435688527675040625802564660517371780402481729089536555417949051890403879840079255169295922593080322634775209689623239873322471161642996440906533187938298969649928516003704476137795166849228875
        )

    b.setup(queue, store, store, PickleCodec())
    await b.schedule(TOPIC, "top", (), {})
    async with b.wrrrk() as c:
        # Terrible hack: because we don’t do proper parent debouncing, this stress
        # test ends up with a metric ton of duplicate calls.
        async def wait_and_close():
            await asyncio.sleep(1)
            await queue.close()

        await asyncio.gather(*([c.loop(TOPIC) for _ in range(10)] + [wait_and_close()]))
    await queue.join()


async def test_debounce_child():
    b = Brrr()
    calls = Counter()
    store = InMemoryByteStore()
    queue = ClosableInMemQueue([TOPIC])

    @b.task
    async def foo(a: int) -> int:
        calls[a] += 1
        if a == 0:
            return a

        ret = sum(await b.gather(*map(foo, [a - 1] * 50)))
        if a == 3:
            await queue.close()
        return ret

    b.setup(queue, store, store, PickleCodec())
    await b.schedule(TOPIC, "foo", (3,), {})
    async with b.wrrrk() as c:
        await c.loop(TOPIC)
    await queue.join()
    assert calls == Counter({0: 1, 1: 2, 2: 2, 3: 2})


# This formalizes an anti-feature: we actually do want to debounce calls to the
# same parent.  Let’s at least be explicit about this for now.
async def test_no_debounce_parent():
    b = Brrr()
    calls = Counter()
    store = InMemoryByteStore()
    queue = ClosableInMemQueue([TOPIC])

    @b.task
    async def one(_: int) -> int:
        calls["one"] += 1
        return 1

    @b.task
    async def foo(a: int) -> int:
        calls["foo"] += 1
        # Different argument to avoid debouncing children
        ret = sum(await b.gather(*map(one, range(a))))
        # Obviously we only actually ever want to reach this point once
        if calls["foo"] == 1 + a:
            await queue.close()
        return ret

    b.setup(queue, store, store, PickleCodec())
    await b.schedule(TOPIC, "foo", (50,), {})
    async with b.wrrrk() as c:
        await c.loop(TOPIC)
    await queue.join()
    # We want foo=2 here
    assert calls == Counter(one=50, foo=51)


async def test_wrrrk_recoverable():
    b = Brrr()
    queue = ClosableInMemQueue([TOPIC])
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
        async with b.wrrrk() as c:
            await c.loop(TOPIC)
    except MyError:
        my_error_encountered = True
    assert my_error_encountered

    # Trick the test queue implementation to survive this
    queue.queues[TOPIC] = asyncio.Queue()
    await b.schedule(TOPIC, "bar", (2,), {})
    async with b.wrrrk() as c:
        await c.loop(TOPIC)
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


def test_get_tasks():
    b = Brrr()

    @b.task
    async def foo(a: int) -> int:
        return a

    @b.task
    async def bar(a: int) -> int:
        return a * a

    assert b.tasks == {"bar": bar, "foo": foo}


def test_task_setup():
    b = Brrr()

    @b.task(name="zim")
    async def foo(a: int) -> int:
        return a

    @b.task
    async def bar(a: int) -> int:
        return a * a

    assert foo.name == "zim"
    assert bar.name == "bar"
