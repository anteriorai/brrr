from collections.abc import Sequence
from contextlib import asynccontextmanager
from typing import AsyncIterator

from brrr.queue import Queue
from brrr.backends.in_memory import InMemoryQueue
from tests.contract_queue import QueueContract


class TestInMemoryQueue(QueueContract):
    has_accurate_info = True

    @asynccontextmanager
    async def with_queue(self, topics: Sequence[str]) -> AsyncIterator[Queue]:
        queue = InMemoryQueue(topics=topics)
        queue.recv_block_secs = 1
        yield queue
