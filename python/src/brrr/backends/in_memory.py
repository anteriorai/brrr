from __future__ import annotations

import collections
from collections.abc import MutableMapping
import typing

from brrr.store import CompareMismatch
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
    return f"{key.type}/{key.id}"


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
        return self.inner[_key2str(key)]

    async def set(self, key: MemKey, value: bytes):
        self.inner[_key2str(key)] = value

    async def delete(self, key: MemKey):
        try:
            del self.inner[_key2str(key)]
        except KeyError:
            pass

    async def set_new_value(self, key: MemKey, value: bytes):
        k = _key2str(key)
        if k in self.inner:
            raise CompareMismatch
        self.inner[k] = value

    async def compare_and_set(self, key: MemKey, value: bytes, expected: bytes):
        k = _key2str(key)
        if (k not in self.inner) or (self.inner[k] != expected):
            raise CompareMismatch
        self.inner[k] = value

    async def compare_and_delete(self, key: MemKey, expected: bytes):
        k = _key2str(key)
        if (k not in self.inner) or (self.inner[k] != expected):
            raise CompareMismatch
        del self.inner[k]

    async def incr(self, k: str) -> int:
        n = self.inner.get(k, 0) + 1
        self.inner[k] = n
        return n
