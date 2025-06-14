from __future__ import annotations

import asyncio
import base64
from collections.abc import (
    AsyncIterator,
    Awaitable,
    Callable,
    Iterable,
    Mapping,
    MutableMapping,
    MutableSequence,
    Sequence,
)
from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import dataclass
import functools
import logging
from typing import Any, overload
from uuid import uuid4

from .call import Call
from .store import (
    AlreadyExists,
    Cache,
    Codec,
    Memory,
    Store,
)
from .queue import Queue, QueueIsClosed, QueueIsEmpty

logger = logging.getLogger(__name__)

type _TaskDecorator[**P, R] = Callable[[Callable[P, Awaitable[R]]], Task[P, R]]


class SpawnLimitError(Exception):
    pass


class NotInBrrrError(Exception):
    """Trying to access worker context from outside a worker"""

    pass


@dataclass
class DeferredCall:
    # None means self
    topic: str | None
    call: Call


class Defer(Exception):
    """
    When a task is called and hasn't been computed yet, a Defer exception is raised
    Workers catch this exception and schedule the task to be computed
    """

    calls: Sequence[DeferredCall]

    def __init__(self, calls: Sequence[DeferredCall]):
        self.calls = calls


class Task[**P, R]:
    """
    A decorator to turn a function into a task.
    When it is called, within the context of a worker, it checks whether it has already been computed.
    If so, it returns the value, otherwise it raises a Call job, which causes the worker to schedule the computation.

    A task can not write to the store, only read from it
    """

    def __init__(
        self,
        brrr: Brrr,
        fn: Callable[P, Awaitable[R]],
        name: str | None = None,
    ):
        self.brrr = brrr
        self.fn = fn
        self.name = name or fn.__name__

    # Calling a function returns the value if it has already been computed.
    # Otherwise, it raises a Call exception to schedule the computation
    async def __call__(self, *args, **kwargs):
        wrrrkr = self.brrr._worker.get(None)
        if wrrrkr is None:
            return await self.evaluate(args, kwargs)
        return await wrrrkr.call(None, self.name)(*args, **kwargs)

    async def evaluate(self, args, kwargs):
        return await self.fn(*args, **kwargs)


class Brrr:
    """
    All state for brrr to function wrapped in a container.
    """

    # Tasks pending registration before setup.  This allows for a very lazy API
    # which is particularly polite when asking users to put task registration as
    # a top-level decorator: only do things (like raise errors!) if you actually
    # _need_ brrr, not e.g. in a unit test which doesn’t use it.
    _registering_tasks: MutableSequence[Task]

    _worker: ContextVar[Wrrrker]

    def __init__(self):
        self._registering_tasks = []
        self._worker = ContextVar("brrr.worker")

    @asynccontextmanager
    async def connect(
        self, queue: Queue, store: Store, cache: Cache, codec: Codec
    ) -> AsyncIterator[Client]:
        """Create a client-only connection without the ability to handle jobs.

        It can schedule new tasks and read values from the kv store though!

        """
        yield Client(self, queue, store, cache, codec)

    @asynccontextmanager
    async def wrrrk(
        self,
        queue: Queue,
        store: Store,
        cache: Cache,
        codec: Codec,
        enrich_task: Callable[[Task], Task] = lambda x: x,
    ) -> AsyncIterator[Wrrrker]:
        """Create a worker from this connection, to support _handling_ jobs"""

        def add_task(
            agg: MutableMapping[str, Task], task: Task
        ) -> MutableMapping[str, Task]:
            if task.name in agg:
                raise Exception(f"Task {task.name} already exists")
            agg[task.name] = task
            return agg

        tasks: MutableMapping[str, Task] = functools.reduce(
            add_task, map(enrich_task, self._registering_tasks), {}
        )
        yield Wrrrker(self, queue, store, cache, codec, tasks)

    @overload
    def task[**P, R](self, fn: Callable[P, Awaitable[R]]) -> Task[P, R]: ...
    @overload
    def task[**P, R](self, *, name: str | None = None) -> _TaskDecorator[P, R]: ...

    def task(self, *args, **kwargs):
        def _wrap(fn):
            if self._registering_tasks is None:
                # Technically I guess you could but let’s not get into the habit.
                raise Exception("Cannot register tasks after Brrr.setup called")
            task = Task(self, fn, **tkwargs)
            self._registering_tasks.append(task)
            return task

        if len(args) == 1 and callable(args[0]) and not kwargs:
            defaults = {"name": None}
            tkwargs = defaults | kwargs
            return _wrap(args[0])
        elif (not args) and kwargs:
            tkwargs = kwargs
            return _wrap
        else:
            raise ValueError("Unknown call signature for task decorator")

    def worker(self) -> Wrrrker:
        try:
            return self._worker.get()
        except LookupError:
            raise NotInBrrrError()


class Client:
    """Put work and read from brrr but don't handle any tasks"""

    # Maximum task executions per root job.  Hard-coded, not intended to be
    # configurable or ever even be hit, for that matter.  If you hit this you almost
    # certainly have a pathological workflow edge case causing massive reruns.  If
    # you actually need to increase this because your flows genuinely hit this
    # limit, I’m impressed.
    _spawn_limit: int = 10_000

    _codec: Codec

    # Non-critical, non-persistent information.  Still figuring out if it makes
    # sense to have this dichotomy supported so explicitly at the top-level of
    # the API.  We run the risk of somehow letting semantically important
    # information seep into this cache, and suddenly it is effectively just part
    # of memory again, at which point what’s the split for?
    _cache: Cache
    # A storage backend for calls, values and pending returns
    _memory: Memory
    # A queue of call keys to be processed
    _queue: Queue

    _brrr: Brrr

    def __init__(
        self, brrr: Brrr, queue: Queue, store: Store, cache: Cache, codec: Codec
    ):
        self._brrr = brrr
        self._codec = codec
        self._cache = cache
        self._memory = Memory(store, codec)
        self._queue = queue

    async def _put_job(self, topic: str, call_hash: str, root_id: str):
        # Incredibly mother-of-all ad-hoc definitions.  Doesn’t use the topic
        # for counting spawn limits: the spawn limit is currently intended to
        # never be hit at all: it’s a /semantic/ check, not a /runtime/ check.
        # It’s not intended for example to give paying customers a higher spawn
        # limit than free ones.  It’s intended to catch infinite recursion and
        # non-idempotent call graphs.
        if (await self._cache.incr(f"brrr_count/{root_id}")) > self._spawn_limit:
            msg = f"Spawn limit {self._spawn_limit} reached for {root_id} at job {call_hash}"
            logger.error(msg)
            # Throw here because it allows the user of brrrlib to decide how to
            # handle this: what kind of logging?  Does the worker crash in order
            # to flag the problem to the service orchestrator, relying on auto
            # restarts to maintain uptime while allowing monitoring to go flag a
            # bigger issue to admins?  Or just wrap it in a while True loop
            # which catches and ignores specifically this error?
            raise SpawnLimitError(msg)
        await self._queue.put_message(topic, f"{root_id}/{call_hash}")

    async def _schedule_call_root(self, topic: str, call: Call):
        """Schedule this call on the brrr workforce.

        This is the real internal entrypoint which should be used by all brrr
        internal-facing code, to avoid confusion about what's internal API and
        what's external.

        This method should be called for top-level workflow calls only.

        """
        await self._memory.set_call(call)
        # Random root id for every call so we can disambiguate retries
        root_id = base64.urlsafe_b64encode(uuid4().bytes).decode("ascii").strip("=")
        await self._put_job(topic, call.call_hash, root_id)

    async def read(self, task_name: str, args: tuple, kwargs: dict):
        """
        Returns the value of a task, or raises a KeyError if it's not present in the store
        """
        call = self._memory.make_call(task_name, args, kwargs)
        encoded_val = await self._memory.get_value(call)
        return self._codec.decode_return(encoded_val)

    def schedule(self, topic: str, task_name: str):
        """Public-facing one-shot schedule method.

        The exact API for the type of args and kwargs is still WIP.  We're doing
        (args, kwargs) for now but it's WIP.

        Don't use this internally.

        """

        async def f(*args, **kwargs):
            call = self._memory.make_call(task_name, args, kwargs)
            if await self._memory.has_value(call):
                return

            return await self._schedule_call_root(topic, call)

        return f


class Wrrrker(Client):
    """A client which can also _handle_ jobs"""

    # Dictionary of task_name to task instance
    tasks: Mapping[str, Task]
    n: int

    def __init__(
        self,
        brrr: Brrr,
        queue: Queue,
        store: Store,
        cache: Cache,
        codec: Codec,
        tasks: Mapping[str, Task],
    ):
        super().__init__(brrr, queue, store, cache, codec)
        self.tasks = tasks
        self.n = 0

    def _parse_call_id(self, call_id: str):
        return call_id.split("/")

    async def _call_task(self, task_name: str, memo_key: str, payload: bytes) -> Any:
        """
        Evaluate a frame, which means calling the tasks function with its arguments
        """
        task = self.tasks[task_name]
        return await self._codec.invoke_task(memo_key, task.name, task.fn, payload)

    async def _handle_msg(self, my_topic: str, my_call_id: str):
        root_id, my_memo_key = self._parse_call_id(my_call_id)
        task_name, payload = await self._memory.get_call_bytes(my_memo_key)

        logger.debug(f"Calling {my_topic} -> {my_call_id} -> {task_name}")
        try:
            encoded_ret = await self._call_task(task_name, my_memo_key, payload)
        except Defer as defer:
            logger.debug(
                "Deferring %s %s: %d missing calls",
                my_call_id,
                task_name,
                len(defer.calls),
            )
            # This is very ugly but I want to keep the contract of throwing
            # exceptions on spawn limits, even though it’s _technically_ a user
            # error.  It’s a very nice failure mode and it allows the user to
            # automatically lean on their fleet monitoring to measure the health
            # of their workflows, and debugging this issue can otherwise be very
            # hard.  Of course the “proper” way for a language to support this
            # is Lisp’s restarts, where an exception doesn’t unroll the stack
            # but allows the caller to handle it from the point at which it
            # occurs.
            spawn_limit_err = None

            async def handle_child(child: DeferredCall):
                try:
                    await self._schedule_call_nested(
                        my_topic, child, root_id, my_call_id
                    )
                except SpawnLimitError as e:
                    nonlocal spawn_limit_err
                    spawn_limit_err = e

            await asyncio.gather(*map(handle_child, defer.calls))
            if spawn_limit_err is not None:
                raise spawn_limit_err from defer
            return

        logger.info(f"Resolved {my_topic} -> {my_call_id} -> {task_name}")

        # We can end up in a race against another worker to write the value.  We
        # only accept the first entry and the rest will be ignored.
        try:
            await self._memory.set_value(my_memo_key, encoded_ret)
        except AlreadyExists:
            pass

        # This is ugly and it’s tempting to use asyncio.gather with
        # ‘return_exceptions=True’.  However note I don’t want to blanket catch
        # all errors: only SpawnLimitError.  You’d need to do manual filtering
        # of errors, check if there are any non-spawnlimiterrors, if so throw
        # those immediately from the context block, otherwise throw a spawnlimit
        # error once the context finishes.  It’s about as convoluted as just
        # doing it this way, without any of the clarity.
        spawn_limit_err = None

        async def schedule_returns(returns: Iterable[str]):
            for pending in returns:
                try:
                    await self._schedule_return_call(pending)
                except SpawnLimitError as e:
                    logger.info(
                        f"Spawn limit reached returning from {my_memo_key} to {pending}; clearing the return"
                    )
                    nonlocal spawn_limit_err
                    spawn_limit_err = e

        await self._memory.with_pending_returns_remove(my_memo_key, schedule_returns)
        if spawn_limit_err is not None:
            raise spawn_limit_err

    async def _schedule_call_nested(
        self, my_topic: str, child: DeferredCall, root_id: str, parent_key: str
    ):
        """Schedule this call on the brrr workforce.

        This is the real internal entrypoint which should be used by all brrr
        internal-facing code, to avoid confusion about what's internal API and
        what's external.

        This method is for calls which are scheduled from within another brrr
        call.  When the work scheduled by this call has completed, that worker
        must kick off the parent (which is the thread doing the calling of this
        function, "now").

        This will always kick off the call, it doesn't check if a return value
        already exists for this call.

        """
        # First the call because it is perennial, it just describes the actual
        # call being made, it doesn’t cause any further action and it’s safe
        # under all races.
        await self._memory.set_call(child.call)
        # Note this can be immediately read out by a racing return call. The
        # pathological case is: we are late to a party and another worker is
        # actually just done handling this call, and just before it reads out
        # the addresses to which to return, it is added here.  That’s still OK
        # because it will then immediately call this parent flow back, which is
        # fine because the result does in fact exist.
        child_topic = child.topic or my_topic
        memo_key = child.call.call_hash
        schedule_job = functools.partial(self._put_job, child_topic, memo_key, root_id)
        await self._memory.add_pending_return(
            memo_key, f"{my_topic}/{parent_key}", schedule_job
        )

    async def _schedule_return_call(self, return_addr: str):
        # These are all topic/root_id/memo_key triples which is great because
        # every return should be retried in its original root context.
        topic, root_id, parent_key = return_addr.split("/")
        await self._put_job(topic, parent_key, root_id)

    def call(self, topic: str | None, task_name):
        """Directly call a brrr task FROM WITHIN ANOTHER TASK.

        DO NOT call this unless you are, yourself, already inside a brrr task.

        """

        async def f(*args, **kwargs):
            call = self._memory.make_call(task_name, args, kwargs)
            try:
                encoded_val = await self._memory.get_value(call)
            except KeyError:
                raise Defer([DeferredCall(topic, call)])
            else:
                return self._codec.decode_return(encoded_val)

        return f

    # Type annotations for Brrr.gather are modeled after asyncio.gather:
    # support explicit types for 1-5 arguments (and when all have the same type),
    # and a catch-all for the rest.
    @overload
    async def gather[T1](self, coro_or_future1: Awaitable[T1]) -> tuple[T1]: ...
    @overload
    async def gather[T1, T2](
        self, coro_or_future1: Awaitable[T1], coro_or_future2: Awaitable[T2]
    ) -> tuple[T1, T2]: ...
    @overload
    async def gather[T1, T2, T3](
        self,
        coro_or_future1: Awaitable[T1],
        coro_or_future2: Awaitable[T2],
        coro_or_future3: Awaitable[T3],
    ) -> tuple[T1, T2, T3]: ...
    @overload
    async def gather[T1, T2, T3, T4](
        self,
        coro_or_future1: Awaitable[T1],
        coro_or_future2: Awaitable[T2],
        coro_or_future3: Awaitable[T3],
        coro_or_future4: Awaitable[T4],
    ) -> tuple[T1, T2, T3, T4]: ...
    @overload
    async def gather[T1, T2, T3, T4, T5](
        self,
        coro_or_future1: Awaitable[T1],
        coro_or_future2: Awaitable[T2],
        coro_or_future3: Awaitable[T3],
        coro_or_future4: Awaitable[T4],
        coro_or_future5: Awaitable[T5],
    ) -> tuple[T1, T2, T3, T4, T5]: ...
    @overload
    async def gather[T](self, *coro_or_futures: Awaitable[T]) -> list[T]: ...
    @overload
    async def gather(
        self,
        coro_or_future1: Awaitable[Any],
        coro_or_future2: Awaitable[Any],
        coro_or_future3: Awaitable[Any],
        coro_or_future4: Awaitable[Any],
        coro_or_future5: Awaitable[Any],
        *coro_or_futures: Awaitable[Any],
    ) -> list[Any]: ...
    async def gather(self, *task_awaitables):
        """
        Takes a number of task lambdas and calls each of them.
        If they've all been computed, return their values,
        Otherwise raise jobs for those that haven't been computed
        """
        defers: list[DeferredCall] = []
        values = []

        for task_awaitable in task_awaitables:
            try:
                values.append(await task_awaitable)
            except Defer as d:
                defers.extend(d.calls)

        if defers:
            raise Defer(defers)

        return values

    async def loop(self, topic: str):
        """Workers take jobs from the queue, one at a time, and handle them.
        They have read and write access to the store, and are responsible for
        Managing the output of tasks and scheduling new ones

        The default topic on which this brrr instance is _listening_ for new
        jobs.  It can _call_ any jobs in the given queue, but it expects to
        serve its jobs only in this specific queue.

        """
        self.n += 1
        num = self.n
        logger.info(f"Worker {num} listening on {topic}")
        while True:
            try:
                # This is presumed to be a long poll
                message = await self._queue.get_message(topic)
                logger.debug(f"Worker {num} got {topic} message {repr(message)}")
            except QueueIsEmpty:
                logger.debug(f"Worker {num}'s queue {topic} is empty")
                continue
            except QueueIsClosed:
                logger.info(f"Worker {num}'s queue {topic} is closed")
                return

            token = self._brrr._worker.set(self)
            try:
                await self._handle_msg(topic, message.body)
            finally:
                self._brrr._worker.reset(token)
