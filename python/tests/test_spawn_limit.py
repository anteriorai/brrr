from collections import Counter

import pytest

import brrr
from brrr import ActiveWorker, AppWorker, SpawnLimitError
from brrr.backends.in_memory import InMemoryByteStore
from brrr.pickle_codec import PickleCodec

from .closable_test_queue import ClosableInMemQueue

TOPIC = "brrr-test"


async def test_spawn_limit_depth():
    queue = ClosableInMemQueue([TOPIC])
    store = InMemoryByteStore()
    n = 0

    @brrr.handler
    async def foo(app: ActiveWorker, a: int) -> int:
        nonlocal n
        n += 1
        if a == 0:
            # Prevent false positives from this test by exiting cleanly at some point
            await queue.close()
            return 0
        return await app.call(foo)(a - 1)

    async with brrr.serve(queue, store, store) as conn:
        conn._spawn_limit = 100
        app = AppWorker(handlers=dict(foo=foo), codec=PickleCodec(), connection=conn)
        await app.schedule("foo", topic=TOPIC)(conn._spawn_limit + 3)

        with pytest.raises(SpawnLimitError):
            await conn.loop(TOPIC, app.handle)

        assert n == conn._spawn_limit


async def test_spawn_limit_breadth_mapped():
    queue = ClosableInMemQueue([TOPIC])
    store = InMemoryByteStore()
    calls = Counter()

    @brrr.handler_no_arg
    async def one(_: int) -> int:
        calls["one"] += 1
        return 1

    @brrr.handler
    async def foo(app: ActiveWorker, a: int) -> int:
        calls["foo"] += 1
        # Pass a different argument to avoid the debouncer
        val = sum(await app.gather(*map(app.call(one), range(a))))
        # Remove this if-guard when return calls are debounced.
        if calls["foo"] == a + 1:
            await queue.close()
        return val

    async with brrr.serve(queue, store, store) as conn:
        conn._spawn_limit = 100
        app = AppWorker(
            handlers=dict(foo=foo, one=one), codec=PickleCodec(), connection=conn
        )
        await app.schedule("foo", topic=TOPIC)(conn._spawn_limit + 4)

        with pytest.raises(SpawnLimitError):
            await conn.loop(TOPIC, app.handle)

    assert calls["foo"] == 1


async def test_spawn_limit_recoverable():
    queue = ClosableInMemQueue([TOPIC])
    store = InMemoryByteStore()
    cache = InMemoryByteStore()
    calls = Counter()

    @brrr.handler_no_arg
    async def one(_: int) -> int:
        calls["one"] += 1
        return 1

    @brrr.handler
    async def foo(app: ActiveWorker, a: int) -> int:
        calls["foo"] += 1
        # Pass a different argument to avoid the debouncer
        val = sum(await app.gather(*map(app.call(one), range(a))))
        # Remove this if-guard when return calls are debounced.
        if calls["foo"] == a + 1:
            await queue.close()
        return val

    async with brrr.serve(queue, store, cache) as conn:
        conn._spawn_limit = 100
        spawn_limit_encountered = False
        n = conn._spawn_limit + 1
        app = AppWorker(
            handlers=dict(foo=foo, one=one), codec=PickleCodec(), connection=conn
        )
        await app.schedule("foo", topic=TOPIC)(n)

        while True:
            # Very ugly but this works for testing
            cache.inner = {}
            try:
                await conn.loop(TOPIC, app.handle)
                break
            except SpawnLimitError:
                spawn_limit_encountered = True

    # I expect messages to be left pending as unhandled here, that’s the point:
    assert spawn_limit_encountered
    # Once we debounce parent calls this should be foo=2
    assert calls == Counter(dict(one=n, foo=n + 1))


async def test_spawn_limit_breadth_manual():
    queue = ClosableInMemQueue([TOPIC])
    store = InMemoryByteStore()
    calls = Counter()

    @brrr.handler_no_arg
    async def one(_: int) -> int:
        calls["one"] += 1
        return 1

    @brrr.handler
    async def foo(app: ActiveWorker, a: int) -> int:
        calls["foo"] += 1
        total = 0
        for i in range(a):
            # Pass a different argument to avoid the debouncer
            total += await app.call(one)(i)

        await queue.close()
        return total

    async with brrr.serve(queue, store, store) as conn:
        conn._spawn_limit = 100
        app = AppWorker(
            handlers=dict(foo=foo, one=one), codec=PickleCodec(), connection=conn
        )
        await app.schedule("foo", topic=TOPIC)(conn._spawn_limit + 3)
        with pytest.raises(SpawnLimitError):
            await conn.loop(TOPIC, app.handle)

        assert calls == Counter(
            dict(one=conn._spawn_limit / 2, foo=conn._spawn_limit / 2)
        )


async def test_spawn_limit_cached():
    queue = ClosableInMemQueue([TOPIC])
    store = InMemoryByteStore()
    n = 0
    final = None

    @brrr.handler_no_arg
    async def same(a: int) -> int:
        nonlocal n
        n += 1
        return a

    @brrr.handler
    async def foo(app: ActiveWorker, a: int) -> int:
        val = sum(await app.gather(*map(app.call(same), [1] * a)))
        await queue.close()
        nonlocal final
        final = val
        return val

    async with brrr.serve(queue, store, store) as conn:
        conn._spawn_limit = 100
        app = AppWorker(
            handlers=dict(foo=foo, same=same), codec=PickleCodec(), connection=conn
        )
        await app.schedule("foo", topic=TOPIC)(conn._spawn_limit + 5)
        await conn.loop(TOPIC, app.handle)
        await queue.join()

        assert n == 1
        assert final == conn._spawn_limit + 5
