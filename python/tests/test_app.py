import asyncio
from collections import Counter
import dataclasses
import typing
from typing import cast

from brrr.local_app import local_app
import pytest

import brrr
from brrr import ActiveWorker, AppWorker, AppConsumer
from brrr.backends.in_memory import InMemoryByteStore, InMemoryQueue
from brrr import Connection, Defer, DeferredCall, NotFoundError, Request, Response
from brrr.pickle_codec import PickleCodec

TOPIC = "brrr-test"


async def test_app_worker() -> None:
    store = InMemoryByteStore()
    queue = InMemoryQueue([TOPIC])

    @brrr.handler_no_arg
    async def bar(a: int) -> int:
        assert a == 123
        return 456

    @brrr.handler
    async def foo(app: ActiveWorker, a: int) -> int:
        return await app.call(bar, topic=TOPIC)(a + 1) + 1

    async with brrr.serve(queue, store, store) as conn:
        app = AppWorker(
            handlers=dict(foo=foo, bar=bar), codec=PickleCodec(), connection=conn
        )
        await app.schedule(foo, topic=TOPIC)(122)
        queue.flush()
        await conn.loop(TOPIC, app.handle)
        assert await app.read(foo)(122) == 457
        assert await app.read("foo")(122) == 457
        assert await app.read(bar)(123) == 456
        assert await app.read("bar")(123) == 456


async def test_app_consumer() -> None:
    store = InMemoryByteStore()
    queue = InMemoryQueue([TOPIC])

    @brrr.handler_no_arg
    async def foo(a: int) -> int:
        return a * a

    # Seed the db with a known value
    async with brrr.serve(queue, store, store) as conn:
        appw = AppWorker(handlers=dict(foo=foo), codec=PickleCodec(), connection=conn)
        await appw.schedule(foo, topic=TOPIC)(5)
        queue.flush()
        await conn.loop(TOPIC, appw.handle)

    # Now test that a read-only app can read that
    async with brrr.serve(queue, store, store) as conn:
        appc = AppConsumer(codec=PickleCodec(), connection=conn)
        assert await appc.read("foo")(5) == 25
        with pytest.raises(NotFoundError):
            await appc.read("foo")(3)
        with pytest.raises(NotFoundError):
            await appc.read("bar")(5)


async def test_local_app() -> None:
    @brrr.handler_no_arg
    async def bar(a: int) -> int:
        assert a == 123
        return 456

    @brrr.handler
    async def foo(app: ActiveWorker, a: int) -> int:
        return await app.call(bar, topic=TOPIC)(a + 1) + 1

    async with local_app(
        topic=TOPIC, handlers=dict(foo=foo, bar=bar), codec=PickleCodec()
    ) as app:
        await app.schedule(foo)(122)
        await app.run()
        assert await app.read(foo)(122) == 457


async def _call_nested_gather(*, use_brrr_gather: bool) -> list[str]:
    """
    Helper function to test that brrr.gather runs all brrr tasks in parallel,
    in contrast with how asyncio.gather only runs one at a time.
    """
    calls = []

    @brrr.handler_no_arg
    async def foo(a: int) -> int:
        calls.append(f"foo({a})")
        return a * 2

    @brrr.handler_no_arg
    async def bar(a: int) -> int:
        calls.append(f"bar({a})")
        return a - 1

    async def not_a_brrr_task(app: ActiveWorker, a: int) -> int:
        b = await app.call(foo)(a)
        return await app.call(bar)(b)

    @brrr.handler
    async def top(app: ActiveWorker, xs: list[int]) -> list[int]:
        calls.append(f"top({xs})")
        gather = app.gather if use_brrr_gather else asyncio.gather
        result = await gather(*[not_a_brrr_task(app, x) for x in xs])
        typing.assert_type(result, list[int])
        return result

    handlers = dict(foo=foo, bar=bar, top=top)
    async with local_app(topic=TOPIC, handlers=handlers, codec=PickleCodec()) as app:
        await app.schedule(top)([3, 4])
        await app.run()

    return calls


async def test_app_gather() -> None:
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


async def test_asyncio_gather() -> None:
    """
    Since asyncio.gather raises the first Defer, top should Defer four times.
    Each foo call should happen before its logical next bar call, but there is no
    guarantee that either foo call happens before the other bar call.
    """
    asyncio_calls = await _call_nested_gather(use_brrr_gather=False)
    assert len([c for c in asyncio_calls if c.startswith("top")]) == 5
    assert asyncio_calls.index("foo(3)") < asyncio_calls.index("bar(6)")
    assert asyncio_calls.index("foo(4)") < asyncio_calls.index("bar(8)")


async def test_topics_separate_app_same_conn() -> None:
    store = InMemoryByteStore()
    queue = InMemoryQueue(["t1", "t2"])

    @brrr.handler_no_arg
    async def one(a: int) -> int:
        return a + 5

    @brrr.handler
    async def two(app: ActiveWorker, a: int) -> None:
        result = await app.call("one", topic="t1")(a + 3)
        assert result == 15
        await queue.close()

    async with brrr.serve(queue, store, store) as conn:
        app1 = AppWorker(handlers=dict(one=one), codec=PickleCodec(), connection=conn)
        app2 = AppWorker(handlers=dict(two=two), codec=PickleCodec(), connection=conn)
        await app2.schedule("two", topic="t2")(7)
        await asyncio.gather(conn.loop("t1", app1.handle), conn.loop("t2", app2.handle))

    await queue.join()


async def test_topics_separate_app_separate_conn() -> None:
    store = InMemoryByteStore()
    queue = InMemoryQueue(["t1", "t2"])

    @brrr.handler_no_arg
    async def one(a: int) -> int:
        return a + 5

    @brrr.handler
    async def two(app: ActiveWorker, a: int) -> None:
        result = await app.call("one", topic="t1")(a + 3)
        assert result == 15
        await queue.close()

    async with brrr.serve(queue, store, store) as conn1:
        async with brrr.serve(queue, store, store) as conn2:
            app1 = AppWorker(
                handlers=dict(one=one), codec=PickleCodec(), connection=conn1
            )
            app2 = AppWorker(
                handlers=dict(two=two), codec=PickleCodec(), connection=conn2
            )
            await app2.schedule("two", topic="t2")(7)
            await asyncio.gather(
                conn1.loop("t1", app1.handle), conn2.loop("t2", app2.handle)
            )

    await queue.join()


async def test_topics_same_app() -> None:
    store = InMemoryByteStore()
    queue = InMemoryQueue(["t1", "t2"])

    @brrr.handler_no_arg
    async def one(a: int) -> int:
        return a + 5

    @brrr.handler
    async def two(app: ActiveWorker, a: int) -> None:
        # N.B.: b2 can use its own brrr instance
        result = await app.call("one", topic="t1")(a + 3)
        assert result == 15
        await queue.close()

    async with brrr.serve(queue, store, store) as conn:
        app = AppWorker(
            handlers=dict(one=one, two=two), codec=PickleCodec(), connection=conn
        )
        await app.schedule("two", topic="t2")(7)
        # Listen on different topics with the same worker.
        await asyncio.gather(conn.loop("t1", app.handle), conn.loop("t2", app.handle))

    await queue.join()


async def test_app_nop_closed_queue() -> None:
    store = InMemoryByteStore()
    queue = InMemoryQueue([TOPIC])
    await queue.close()
    async with brrr.serve(queue, store, store) as conn:
        app = AppWorker(handlers={}, codec=PickleCodec(), connection=conn)
        await conn.loop(TOPIC, app.handle)
        await conn.loop(TOPIC, app.handle)
        await conn.loop(TOPIC, app.handle)


async def test_stop_when_empty() -> None:
    # Keeping state of the calls to see how often it’s called
    calls_pre = Counter[int]()
    calls_post = Counter[int]()
    store = InMemoryByteStore()
    queue = InMemoryQueue([TOPIC])

    @brrr.handler
    async def foo(app: ActiveWorker, a: int) -> int:
        calls_pre[a] += 1
        if a == 0:
            return 0
        res = await app.call(foo)(a - 1)
        calls_post[a] += 1
        return res

    async with brrr.serve(queue, store, store) as conn:
        app = AppWorker(handlers=dict(foo=foo), codec=PickleCodec(), connection=conn)
        await app.schedule(foo, topic=TOPIC)(3)
        queue.flush()
        await conn.loop(TOPIC, app.handle)
        await queue.join()

    assert calls_pre == Counter({0: 1, 1: 2, 2: 2, 3: 2})
    assert calls_post == Counter({1: 1, 2: 1, 3: 1})


@pytest.mark.parametrize("use_gather", [(False,), (True,)])
async def test_parallel(use_gather: bool) -> None:
    store = InMemoryByteStore()
    queue = InMemoryQueue([TOPIC])

    parallel = 5
    barrier: asyncio.Barrier | None = asyncio.Barrier(parallel)

    top_calls = 0

    @brrr.handler_no_arg
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

    @brrr.handler
    async def top(app: ActiveWorker) -> None:
        gather = app.gather if use_gather else asyncio.gather
        await gather(*(app.call(block)(x) for x in range(parallel)))

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

    async with brrr.serve(queue, store, store) as conn:
        app = AppWorker(
            handlers=dict(top=top, block=block), codec=PickleCodec(), connection=conn
        )
        # Don’t use queue.flush() because this test uses parallel workers
        await app.schedule(top, topic=TOPIC)()
        await asyncio.gather(*(conn.loop(TOPIC, app.handle) for _ in range(parallel)))
        await queue.join()


async def test_stress_parallel() -> None:
    store = InMemoryByteStore()
    queue = InMemoryQueue([TOPIC])

    @brrr.handler
    async def fib(app: ActiveWorker, a: int) -> int:
        if a < 2:
            return a
        return sum(
            await app.gather(
                app.call(fib)(a - 1),
                app.call(fib)(a - 2),
            )
        )

    @brrr.handler
    async def top(app: ActiveWorker) -> None:
        n = await app.call(fib)(1000)
        assert (
            n
            == 43466557686937456435688527675040625802564660517371780402481729089536555417949051890403879840079255169295922593080322634775209689623239873322471161642996440906533187938298969649928516003704476137795166849228875
        )

    async with brrr.serve(queue, store, store) as conn:
        app = AppWorker(
            handlers=dict(top=top, fib=fib), codec=PickleCodec(), connection=conn
        )
        await app.schedule(top, topic=TOPIC)()

        # Terrible hack: because we don’t do proper parent debouncing, this stress
        # test ends up with a metric ton of duplicate calls.
        async def wait_and_close() -> None:
            await asyncio.sleep(1)
            await queue.close()

        await asyncio.gather(
            *([conn.loop(TOPIC, app.handle) for _ in range(10)] + [wait_and_close()])
        )
        await queue.join()


async def test_debounce_child() -> None:
    calls = Counter[int]()

    @brrr.handler
    async def foo(app: ActiveWorker, a: int) -> int:
        calls[a] += 1
        if a == 0:
            return a

        return sum(await app.gather(*map(app.call(foo), [a - 1] * 50)))

    async with local_app(
        topic=TOPIC, handlers=dict(foo=foo), codec=PickleCodec()
    ) as app:
        await app.schedule(foo)(3)
        await app.run()

    assert calls == Counter({0: 1, 1: 2, 2: 2, 3: 2})


# This formalizes an anti-feature: we actually do want to debounce calls to the
# same parent.  Let’s at least be explicit about this for now.
async def test_no_debounce_parent() -> None:
    calls = Counter[str]()

    @brrr.handler_no_arg
    async def one(_: int) -> int:
        calls["one"] += 1
        return 1

    @brrr.handler
    async def foo(app: ActiveWorker, a: int) -> int:
        calls["foo"] += 1
        # Different argument to avoid debouncing children
        return sum(await app.gather(*map(app.call(one), range(a))))

    async with local_app(
        topic=TOPIC, handlers=dict(one=one, foo=foo), codec=PickleCodec()
    ) as app:
        await app.schedule(foo)(50)
        await app.run()

    # We want foo=2 here
    assert calls == Counter(one=50, foo=51)


async def test_app_loop_resumable() -> None:
    store = InMemoryByteStore()
    queue = InMemoryQueue([TOPIC])

    errors = 5

    class MyError(Exception):
        pass

    @brrr.handler_no_arg
    async def foo(a: int) -> int:
        nonlocal errors
        if errors:
            errors -= 1
            raise MyError("retry")
        await queue.close()
        return a

    async with brrr.serve(queue, store, store) as conn:
        app = AppWorker(handlers=dict(foo=foo), codec=PickleCodec(), connection=conn)
        while True:
            try:
                await app.schedule(foo, topic=TOPIC)(3)
                await conn.loop(TOPIC, app.handle)
                break
            except MyError:
                continue

    await queue.join()
    assert errors == 0


async def test_app_handler_names() -> None:
    @brrr.handler_no_arg
    async def foo(a: int) -> int:
        return a * a

    @brrr.handler
    async def bar(app: ActiveWorker, a: int) -> int:
        # Both are the same.
        return await app.call(foo)(a) * cast(int, await app.call("quux/zim")(a))

    handlers = {
        "quux/zim": foo,
        "quux/bar": bar,
    }
    async with local_app(topic=TOPIC, handlers=handlers, codec=PickleCodec()) as app:
        await app.schedule("quux/bar")(4)
        await app.run()
        assert await app.read("quux/zim")(4) == 16
        assert await app.read(foo)(4) == 16


async def test_app_subclass() -> None:
    store = InMemoryByteStore()
    queue = InMemoryQueue([TOPIC])

    @brrr.handler_no_arg
    async def bar(a: int) -> int:
        return a + 1

    @brrr.handler_no_arg
    async def baz(a: int) -> int:
        return a + 10

    @brrr.handler
    async def foo(app: ActiveWorker, a: int) -> int:
        return await app.call(bar)(a)

    # Hijack any defers and change them to a different task.  Just to prove a
    # point about middleware, nothing particularly realistic.
    class MyAppWorker(AppWorker):
        async def handle(self, request: Request, conn: Connection) -> Response | Defer:
            resp = await super().handle(request, conn)
            if isinstance(resp, Response):
                return resp

            assert isinstance(resp, Defer)

            def change_defer(d: DeferredCall) -> DeferredCall:
                return dataclasses.replace(
                    d, call=dataclasses.replace(d.call, task_name="baz")
                )

            return Defer(calls=map(change_defer, resp.calls))

    handlers = dict(foo=foo, bar=bar, baz=baz)
    async with brrr.serve(queue, store, store) as conn:
        app = MyAppWorker(handlers=handlers, codec=PickleCodec(), connection=conn)
        await app.schedule(foo, topic=TOPIC)(4)
        queue.flush()
        await conn.loop(TOPIC, app.handle)
        assert await app.read(foo)(4) == 14
