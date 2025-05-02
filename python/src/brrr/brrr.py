from __future__ import annotations

import asyncio
import base64
from collections.abc import Awaitable, Callable
import functools
import logging
from typing import Any, Concatenate, overload
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


class SpawnLimitError(Exception):
    pass


class Defer(Exception):
    """
    When a task is called and hasn't been computed yet, a Defer exception is raised
    Workers catch this exception and schedule the task to be computed
    """

    calls: list[Call]

    def __init__(self, calls: list[Call]):
        self.calls = calls


class Brrr:
    """
    All state for brrr to function wrapped in a container.
    """

    @staticmethod
    def requires_setup[**P, R](
        method: Callable[Concatenate[Brrr, P], R],
    ) -> Callable[Concatenate[Brrr, P], R]:
        @functools.wraps(method)
        def wrapper(self: Brrr, *args: P.args, **kwargs: P.kwargs) -> R:
            if self.queue is None or self.memory is None or self.cache is None:
                raise Exception("Brrr not set up")
            return method(self, *args, **kwargs)

        return wrapper

    # The worker loop (as of writing) is synchronous so it can safely set a
    # local global variable to indicate that it is a worker thread, which, for
    # tasks, means that their Defer raises will be caught and handled by the
    # worker
    worker_singleton: Wrrrker | None

    # Non-critical, non-persistent information.  Still figuring out if it makes
    # sense to have this dichotomy supported so explicitly at the top-level of
    # the API.  We run the risk of somehow letting semantically important
    # information seep into this cache, and suddenly it is effectively just part
    # of memory again, at which point what’s the split for?
    cache: Cache
    # A storage backend for calls, values and pending returns
    memory: Memory
    # A queue of call keys to be processed
    queue: Queue

    # Dictionary of task_name to task instance
    tasks: dict[str, Task]

    # Maximum task executions per root job.  Hard-coded, not intended to be
    # configurable or ever even be hit, for that matter.  If you hit this you almost
    # certainly have a pathological workflow edge case causing massive reruns.  If
    # you actually need to increase this because your flows genuinely hit this
    # limit, I’m impressed.
    _spawn_limit: int

    def __init__(self):
        self.cache = None
        self.memory = None
        self.queue = None
        self.tasks = {}
        self.worker_singleton = None
        self._spawn_limit = 10_000

    # TODO Do we want to pass in a memstore/kv instead?
    def setup(self, queue: Queue, store: Store, cache: Cache, codec: Codec):
        """Initialize the dependencies used by this brrr instance.

        Currently extremely explicit about its inputs while we're still figuring
        out the API.

        """
        self._codec = codec
        self.cache = cache
        self.memory = Memory(store, self._codec)
        self.queue = queue

    def are_we_inside_worker_context(self) -> Any:
        return self.worker_singleton

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
        if not self.are_we_inside_worker_context():
            return await asyncio.gather(*(f for f in task_awaitables))

        defers = []
        values = []

        for task_awaitable in task_awaitables:
            try:
                values.append(await task_awaitable)
            except Defer as d:
                defers.extend(d.calls)

        if defers:
            raise Defer(defers)

        return values

    @requires_setup
    async def schedule(self, task_name: str, args: tuple, kwargs: dict):
        """Public-facing one-shot schedule method.

        The exact API for the type of args and kwargs is still WIP.  We're doing
        (args, kwargs) for now but it's WIP.

        Don't use this internally.

        """
        call = self.memory.make_call(task_name, args, kwargs)
        if await self.memory.has_value(call):
            return

        return await self._schedule_call_root(call)

    @requires_setup
    async def _schedule_call_nested(self, child: Call, root_id: str, parent_key: str):
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
        await self.memory.set_call(child)
        # Note this can be immediately read out by a racing return call. The
        # pathological case is: we are late to a party and another worker is
        # actually just done handling this call, and just before it reads out
        # the addresses to which to return, it is added here.  That’s still OK
        # because it will then immediately call this parent flow back, which is
        # fine because the result does in fact exist.
        schedule_job = functools.partial(self._put_job, child.memo_key, root_id)
        await self.memory.add_pending_return(child.memo_key, parent_key, schedule_job)

    async def _put_job(self, memo_key: str, root_id: str):
        # Incredibly mother-of-all ad-hoc definitions
        if (await self.cache.incr(f"brrr_count/{root_id}")) > self._spawn_limit:
            msg = f"Spawn limit {self._spawn_limit} reached for {root_id} at job {memo_key}"
            logger.error(msg)
            # Throw here because it allows the user of brrrlib to decide how to
            # handle this: what kind of logging?  Does the worker crash in order
            # to flag the problem to the service orchestrator, relying on auto
            # restarts to maintain uptime while allowing monitoring to go flag a
            # bigger issue to admins?  Or just wrap it in a while True loop
            # which catches and ignores specifically this error?
            raise SpawnLimitError(msg)
        await self.queue.put_message(f"{root_id}/{memo_key}")

    @requires_setup
    async def _schedule_call_root(self, call: Call):
        """Schedule this call on the brrr workforce.

        This is the real internal entrypoint which should be used by all brrr
        internal-facing code, to avoid confusion about what's internal API and
        what's external.

        This method should be called for top-level workflow calls only.

        """
        await self.memory.set_call(call)
        # Random root id for every call so we can disambiguate retries
        root_id = base64.urlsafe_b64encode(uuid4().bytes).decode("ascii").strip("=")
        await self._put_job(call.memo_key, root_id)

    @requires_setup
    async def read(self, task_name: str, args: tuple, kwargs: dict):
        """
        Returns the value of a task, or raises a KeyError if it's not present in the store
        """
        call = self.memory.make_call(task_name, args, kwargs)
        encoded_val = await self.memory.get_value(call)
        return self._codec.decode_return(encoded_val)

    @requires_setup
    async def _call_task(self, task_name: str, memo_key: str, payload: bytes) -> bytes:
        """
        Evaluate a frame, which means calling the tasks function with its arguments
        """
        task = self.tasks[task_name]
        return await self._codec.invoke_task(memo_key, task.name, task.fn, payload)

    def register_task[**P, R](
        self, fn: Callable[P, Awaitable[R]], name: str | None = None
    ) -> Task[P, R]:
        task = Task(self, fn, name)
        if task.name in self.tasks:
            raise Exception(f"Task {task.name} already exists")
        self.tasks[task.name] = task
        return task

    def task[**P, R](
        self, fn: Callable[P, Awaitable[R]], name: str | None = None
    ) -> Task[P, R]:
        return Task(self, fn, name)

    async def wrrrk(self):
        """
        Spin up a single brrr worker.
        """
        await Wrrrker(self).loop()


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
    async def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R:
        if not self.brrr.are_we_inside_worker_context():
            return await self.evaluate(args, kwargs)
        call = self.brrr.memory.make_call(self.name, args, kwargs)
        try:
            encoded_val = await self.brrr.memory.get_value(call)
        except KeyError:
            raise Defer([call])
        else:
            return self.brrr._codec.decode_return(encoded_val)

    async def map(self, args: list[dict | list | tuple[tuple, dict]]):
        """
        Fanning out, a map function returns the values if they have already been computed.
        Otherwise, it raises a list of Call exceptions to schedule the computation,
        for the ones that aren't already computed

        Offers a few syntaxes, TBD whether that is useful
        #TODO we _could_ support a list of elements to get passed as a single arg each
        """
        argvs = [
            (arg, {})
            if isinstance(arg, list)
            else ((), arg)
            if isinstance(arg, dict)
            else arg
            for arg in args
        ]
        return await self.brrr.gather(*(self(*argv[0], **argv[1]) for argv in argvs))

    # I think /technically/ the async + await here cancel each other out and you
    # could do without either, but there are so many gotchas around it and
    # possible points of failure that it’s nice to at least ensure this _is_ a
    # coroutine.
    async def evaluate(self, args, kwargs):
        return await self.fn(*args, **kwargs)

    async def schedule(self, *args, **kwargs):
        """
        This puts the task call on the queue, but doesn't return the result!
        """
        return await self.brrr.schedule(self.name, args, kwargs)


class Wrrrker:
    def __init__(self, brrr: Brrr):
        self.brrr = brrr

    # The context manager maintains a thread-local global variable to indicate that the thread is a worker
    # and that any invoked tasks can raise Defer exceptions
    def __enter__(self):
        if self.brrr.worker_singleton is not None:
            raise Exception("Worker already running")
        self.brrr.worker_singleton = self

    def __exit__(self, exc_type, exc_value, traceback):
        self.brrr.worker_singleton = None

    def _parse_call_id(self, call_id: str):
        return call_id.split("/")

    async def _handle_msg(self, my_call_id: str):
        root_id, my_memo_key = self._parse_call_id(my_call_id)
        task_name, payload = await self.brrr.memory.get_call_bytes(my_memo_key)

        logger.debug(f"Calling {task_name}")
        try:
            encoded_ret = await self.brrr._call_task(task_name, my_memo_key, payload)
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

            async def handle_child(child):
                try:
                    await self.brrr._schedule_call_nested(child, root_id, my_call_id)
                except SpawnLimitError as e:
                    nonlocal spawn_limit_err
                    spawn_limit_err = e

            await asyncio.gather(*map(handle_child, defer.calls))
            if spawn_limit_err is not None:
                raise spawn_limit_err from defer
            return

        logger.info("Resolved %s %s", my_call_id, task_name)

        # We can end up in a race against another worker to write the value.  We
        # only accept the first entry and the rest will be ignored.
        try:
            await self.brrr.memory.set_value(my_memo_key, encoded_ret)
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
        async with self.brrr.memory.with_pending_returns_remove(my_memo_key) as returns:
            for pending in returns:
                try:
                    await self._schedule_return_call(pending)
                except SpawnLimitError as e:
                    logger.info(
                        f"Spawn limit reached returning from {my_memo_key} to {pending}; clearing the return"
                    )
                    spawn_limit_err = e
        if spawn_limit_err is not None:
            raise spawn_limit_err

    async def _schedule_return_call(self, parent_id):
        # These are all root_id/memo_key pairs which is great because every
        # return should be retried in its original root context.
        root_id, parent_key = self._parse_call_id(parent_id)
        await self.brrr._put_job(parent_key, root_id)

    async def loop(self):
        """
        Workers take jobs from the queue, one at a time, and handle them.
        They have read and write access to the store, and are responsible for
        Managing the output of tasks and scheduling new ones
        """
        with self:
            logger.info("Worker Started")
            while True:
                try:
                    # This is presumed to be a long poll
                    message = await self.brrr.queue.get_message()
                    logger.debug(f"Got message {repr(message)}")
                except QueueIsEmpty:
                    logger.debug("Queue is empty")
                    continue
                except QueueIsClosed:
                    logger.info("Queue is closed")
                    return

                await self._handle_msg(message.body)
