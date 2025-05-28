from __future__ import annotations

import asyncio
import base64
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
import functools
import logging
from typing import Any
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

# I’d like for the typechecker to raise an error when a value with this type is
# called as a function without await.  How?
AsyncFunc = Callable[..., Awaitable[Any]]


class SpawnLimitError(Exception):
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


class Brrr:
    """
    All state for brrr to function wrapped in a container.
    """

    @staticmethod
    def requires_setup(method):
        @functools.wraps(method)
        def wrapper(self, *args, **kwargs):
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
    # The default topic on which this brrr instance is _listening_ for new jobs.
    # It can _call_ any jobs in the given queue, but it expects to serve its
    # jobs only in this specific queue.
    topic: str

    # An error which occurred during setup, if any.  Stored by value here and
    # only raised when ‘.setup’ is called to avoid raising errors on import.
    # Particularly useful for people who test code containing brrr tasks,
    # e.g. pytest can want to load a file twice and that should be OK in a test.
    _setup_error: Exception | None

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
        self.topic = None
        self._setup_error = None
        self.tasks = {}
        self.worker_singleton = None
        self._spawn_limit = 10_000

    # TODO Do we want to pass in a memstore/kv instead?
    def setup(self, queue: Queue, store: Store, cache: Cache, codec: Codec):
        """Initialize the dependencies used by this brrr instance.

        Brrr is not usable until this method is called.  This allows you to only
        call .setup in your main() function and keep your code side-effect free
        on import, e.g. for testing.

        If any constraints were violated on registering of tasks, that error is
        only raised when you call this function.  If there were multiple, an
        arbitrary one is raised.

        Currently extremely explicit about its inputs while we're still figuring
        out the API.

        """
        if self._setup_error is not None:
            raise self._setup_error

        self._codec = codec
        self.cache = cache
        self.memory = Memory(store, self._codec)
        self.queue = queue

    def _inside_worker_context_p(self) -> Any:
        return self.worker_singleton

    async def gather(self, *task_awaitables) -> Sequence[Any]:
        """
        Takes a number of task lambdas and calls each of them.
        If they've all been computed, return their values,
        Otherwise raise jobs for those that haven't been computed
        """
        if not self._inside_worker_context_p():
            return await asyncio.gather(*(f for f in task_awaitables))

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

    @requires_setup
    async def schedule(self, topic: str, task_name: str, args: tuple, kwargs: dict):
        """Public-facing one-shot schedule method.

        The exact API for the type of args and kwargs is still WIP.  We're doing
        (args, kwargs) for now but it's WIP.

        Don't use this internally.

        """
        call = self.memory.make_call(task_name, args, kwargs)
        if await self.memory.has_value(call):
            return

        return await self._schedule_call_root(topic, call)

    @requires_setup
    async def call(self, topic: str | None, task_name: str, args: tuple, kwargs: dict):
        """Directly call a brrr task FROM WITHIN ANOTHER TASK.

        DO NOT call this unless you are, yourself, already inside a brrr task.

        """
        if not self._inside_worker_context_p():
            raise ValueError(".call only from within another brrr task")

        call = self.memory.make_call(task_name, args, kwargs)
        try:
            encoded_val = await self.memory.get_value(call)
        except KeyError:
            raise Defer([DeferredCall(topic, call)])
        else:
            return self._codec.decode_return(encoded_val)

    @requires_setup
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
        await self.memory.set_call(child.call)
        # Note this can be immediately read out by a racing return call. The
        # pathological case is: we are late to a party and another worker is
        # actually just done handling this call, and just before it reads out
        # the addresses to which to return, it is added here.  That’s still OK
        # because it will then immediately call this parent flow back, which is
        # fine because the result does in fact exist.
        child_topic = child.topic or my_topic
        memo_key = child.call.memo_key
        schedule_job = functools.partial(self._put_job, child_topic, memo_key, root_id)
        await self.memory.add_pending_return(
            memo_key, f"{my_topic}/{parent_key}", schedule_job
        )

    async def _put_job(self, topic: str, memo_key: str, root_id: str):
        # Incredibly mother-of-all ad-hoc definitions.  Doesn’t use the topic
        # for counting spawn limits: the spawn limit is currently intended to
        # never be hit at all: it’s a /semantic/ check, not a /runtime/ check.
        # It’s not intended for example to give paying customers a higher spawn
        # limit than free ones.  It’s intended to catch infinite recursion and
        # non-idempotent call graphs.
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
        await self.queue.put_message(topic, f"{root_id}/{memo_key}")

    @requires_setup
    async def _schedule_call_root(self, topic: str, call: Call):
        """Schedule this call on the brrr workforce.

        This is the real internal entrypoint which should be used by all brrr
        internal-facing code, to avoid confusion about what's internal API and
        what's external.

        This method should be called for top-level workflow calls only.

        """
        await self.memory.set_call(call)
        # Random root id for every call so we can disambiguate retries
        root_id = base64.urlsafe_b64encode(uuid4().bytes).decode("ascii").strip("=")
        await self._put_job(topic, call.memo_key, root_id)

    @requires_setup
    async def read(self, task_name: str, args: tuple, kwargs: dict):
        """
        Returns the value of a task, or raises a KeyError if it's not present in the store
        """
        call = self.memory.make_call(task_name, args, kwargs)
        encoded_val = await self.memory.get_value(call)
        return self._codec.decode_return(encoded_val)

    @requires_setup
    async def _call_task(self, task_name: str, memo_key: str, payload: bytes) -> Any:
        """
        Evaluate a frame, which means calling the tasks function with its arguments
        """
        task = self.tasks[task_name]
        return await self._codec.invoke_task(memo_key, task.name, task.fn, payload)

    def task(self, fn: AsyncFunc, name: str | None = None) -> Task:
        task = Task(self, fn, name)
        if task.name in self.tasks:
            self._setup_error = Exception(f"Task {task.name} already exists")
        self.tasks[task.name] = task
        return task

    async def wrrrk(self, topic: str):
        """
        Spin up a single brrr worker listening on the given topic.
        """
        await Wrrrker(self).loop(topic)


class Task:
    """
    A decorator to turn a function into a task.
    When it is called, within the context of a worker, it checks whether it has already been computed.
    If so, it returns the value, otherwise it raises a Call job, which causes the worker to schedule the computation.

    A task can not write to the store, only read from it
    """

    fn: AsyncFunc
    name: str
    brrr: Brrr

    def __init__(self, brrr: Brrr, fn: AsyncFunc, name: str | None = None):
        self.brrr = brrr
        self.fn = fn
        self.name = name or fn.__name__

    # Calling a function returns the value if it has already been computed.
    # Otherwise, it raises a Call exception to schedule the computation
    async def __call__(self, *args, **kwargs):
        if not self.brrr._inside_worker_context_p():
            return await self.evaluate(args, kwargs)
        return await self.brrr.call(None, self.name, args, kwargs)

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

    async def _handle_msg(self, my_topic: str, my_call_id: str):
        root_id, my_memo_key = self._parse_call_id(my_call_id)
        task_name, payload = await self.brrr.memory.get_call_bytes(my_memo_key)

        logger.debug(f"Calling {my_topic} -> {my_call_id} -> {task_name}")
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

            async def handle_child(child: DeferredCall):
                try:
                    await self.brrr._schedule_call_nested(
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

    async def _schedule_return_call(self, return_addr: str):
        # These are all topic/root_id/memo_key triples which is great because
        # every return should be retried in its original root context.
        topic, root_id, parent_key = return_addr.split("/")
        await self.brrr._put_job(topic, parent_key, root_id)

    async def loop(self, topic: str):
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
                    message = await self.brrr.queue.get_message(topic)
                    logger.debug(f"Got message {repr(message)}")
                except QueueIsEmpty:
                    logger.debug("Queue is empty")
                    continue
                except QueueIsClosed:
                    logger.info("Queue is closed")
                    return

                await self._handle_msg(topic, message.body)
