from __future__ import annotations

import asyncio
import typing
from collections.abc import Mapping, Sequence
from typing import override

from brrr.store import CompareMismatch, NotFoundError

from ..queue import Message, Queue, QueueInfo, QueueIsClosed, QueueIsEmpty
from ..store import Cache, MemKey, Store


class InMemoryQueue(Queue):
    """In-memory, inherently ephemeral message queue for testing."""

    _queues: Mapping[str, asyncio.Queue[str]]

    def __init__(self, topics: Sequence[str]):
        self._closing = False
        self._flushing = False
        # Could be updated to allow dynamically creating topics on-demand but
        # this is probably a bit nicer for now.
        self._queues = {k: asyncio.Queue() for k in topics}

    async def close(self) -> None:
        """Only works in Python ≥3.13"""
        if self._closing:
            raise ValueError("InMemoryQueue already closed/closing")
        self._closing = True
        for q in self._queues.values():
            q.shutdown()

    async def join(self) -> None:
        """Wait for all queues to be fully closed"""
        await asyncio.gather(*(q.join() for q in self._queues.values()))

    @typing.override
    async def get_message(self, topic: str) -> Message:
        if topic not in self._queues:
            raise ValueError("invalid topic name")

        q = self._queues[topic]
        try:
            if self._flushing:
                try:
                    payload = q.get_nowait()
                except asyncio.QueueEmpty:
                    # Backwards compatible with python 3.12
                    if hasattr(q, "shutdown"):
                        q.shutdown()
                    raise QueueIsClosed()
            else:
                try:
                    async with asyncio.timeout(self.recv_block_secs):
                        payload = await q.get()
                except TimeoutError:
                    raise QueueIsEmpty()
        except asyncio.QueueShutDown:
            raise QueueIsClosed()

        q.task_done()
        return Message(body=payload)

    @typing.override
    async def put_message(self, topic: str, body: str) -> None:
        if topic not in self._queues:
            raise ValueError(f"Unknown topic {topic}")
        await self._queues[topic].put(body)

    async def get_info(self, topic: str) -> QueueInfo:
        return QueueInfo(num_messages=self._queues[topic].qsize())

    def flush(self) -> None:
        """Once the queue is empty, automatically close it.

        Use this for testing: schedule all your test jobs, call .flush(), and
        the jobs themselves will be allowed to fully run to completion, still
        using the queue and scheduling new messages, until all activity has
        stopped.

        This does not work with parallel consumers because one consumer can be
        working on a job while another one does a .get, sees the queue is empty,
        and closes it.

        """
        self._flushing = True


def _key2str(key: MemKey) -> str:
    return f"{key.type}/{key.call_hash}"


# Just to drive the point home
class InMemoryByteStore(Store, Cache):
    """
    A store that stores bytes
    """

    inner: dict[str, bytes]
    cache: dict[str, int]

    def __init__(self) -> None:
        self.inner = {}
        self.cache = {}

    @override
    async def has(self, key: MemKey) -> bool:
        return _key2str(key) in self.inner

    @override
    async def get(self, key: MemKey) -> bytes:
        full_hash = _key2str(key)
        if full_hash not in self.inner:
            raise NotFoundError(key)
        return self.inner[full_hash]

    @override
    async def get_with_retry(self, key: MemKey) -> bytes:
        return await self.get(key=key)

    @override
    async def set(self, key: MemKey, value: bytes) -> None:
        self.inner[_key2str(key)] = value

    @override
    async def delete(self, key: MemKey) -> None:
        try:
            del self.inner[_key2str(key)]
        except KeyError:
            pass

    @override
    async def set_new_value(self, key: MemKey, value: bytes) -> None:
        k = _key2str(key)
        if k in self.inner:
            raise CompareMismatch()
        self.inner[k] = value

    @override
    async def compare_and_set(self, key: MemKey, value: bytes, expected: bytes) -> None:
        k = _key2str(key)
        if (k not in self.inner) or (self.inner[k] != expected):
            raise CompareMismatch()
        self.inner[k] = value

    @override
    async def compare_and_delete(self, key: MemKey, expected: bytes) -> None:
        k = _key2str(key)
        if (k not in self.inner) or (self.inner[k] != expected):
            raise CompareMismatch()
        del self.inner[k]

    @override
    async def incr(self, key: str) -> int:
        n: int = self.cache.get(key, 0) + 1
        self.cache[key] = n
        return n
