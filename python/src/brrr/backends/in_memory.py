from __future__ import annotations

import collections
from collections.abc import MutableMapping
import typing

from brrr.store import AlreadyExistsError, CompareMismatch, NotFoundError
from ..queue import Queue, Message, QueueInfo, QueueIsClosed, QueueIsEmpty
from ..store import Cache, MemKey, Store

if typing.TYPE_CHECKING:
    from typing import Any


class InMemoryQueue(Queue):
    """
    This queue does not do receipts
    """

    messages: MutableMapping[str, collections.deque[str]] = collections.defaultdict(
        collections.deque
    )
    closed = False

    async def put_message(self, topic: str, body: str):
        self.messages[topic].append(body)

    async def get_message(self, topic: str) -> Message:
        if self.closed:
            raise QueueIsClosed
        if not self.messages[topic]:
            raise QueueIsEmpty
        return Message(self.messages[topic].popleft())

    async def get_info(self, topic: str):
        return QueueInfo(num_messages=len(self.messages[topic]))


def _key2str(key: MemKey) -> str:
    return f"{key.type}/{key.call_hash}"


# Just to drive the point home
class InMemoryByteStore(Store, Cache):
    """
    A store that stores bytes
    """

    inner: dict[str, Any]

    def __init__(self):
        self.inner = {}

    async def has(self, key: MemKey) -> bool:
        return _key2str(key) in self.inner

    async def get(self, key: MemKey) -> bytes:
        full_hash = _key2str(key)
        if full_hash not in self.inner:
            raise NotFoundError(key)
        return self.inner[full_hash]

    async def set_first_value(self, key: MemKey, value: bytes) -> None:
        k = _key2str(key)
        if k in self.inner and self.inner[k] != value:
            raise AlreadyExistsError()
        self.inner[_key2str(key)] = value

    async def delete(self, key: MemKey):
        try:
            del self.inner[_key2str(key)]
        except KeyError:
            pass

    async def set_new_value(self, key: MemKey, value: bytes):
        k = _key2str(key)
        if k in self.inner:
            raise CompareMismatch()
        self.inner[k] = value

    async def compare_and_set(self, key: MemKey, value: bytes, expected: bytes):
        k = _key2str(key)
        if (k not in self.inner) or (self.inner[k] != expected):
            raise CompareMismatch()
        self.inner[k] = value

    async def compare_and_delete(self, key: MemKey, expected: bytes):
        k = _key2str(key)
        if (k not in self.inner) or (self.inner[k] != expected):
            raise CompareMismatch()
        del self.inner[k]

    async def incr(self, key: str) -> int:
        n = self.inner.get(key, 0) + 1
        self.inner[key] = n
        return n
