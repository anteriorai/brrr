from collections.abc import Sequence
from abc import ABC, abstractmethod
from contextlib import asynccontextmanager
from typing import AsyncIterator

import pytest

from brrr.queue import Queue, QueueIsEmpty


class QueueContract(ABC):
    has_accurate_info: bool

    @abstractmethod
    @asynccontextmanager
    async def with_queue(self, topics: Sequence[str]) -> AsyncIterator[Queue]:
        """
        A context manager which calls test function f with a queue
        """
        ...

    async def test_queue_raises_empty(self):
        async with self.with_queue(["foo"]) as queue:
            with pytest.raises(QueueIsEmpty):
                await queue.get_message("foo")

    async def test_queue_enqueues(self):
        queue: Queue
        async with self.with_queue(["test-topic"]) as queue:
            messages = {"message-1", "message-2", "message-3"}

            if self.has_accurate_info:
                assert (await queue.get_info("test-topic")).num_messages == 0

            for i, msg in enumerate(messages):
                await queue.put_message("test-topic", msg)
                if self.has_accurate_info:
                    assert (await queue.get_info("test-topic")).num_messages == i + 1

            for i, msg in enumerate(set(messages)):
                message = await queue.get_message("test-topic")
                assert message.body in messages
                messages.remove(message.body)
                if self.has_accurate_info:
                    assert (await queue.get_info("test-topic")).num_messages == len(
                        messages
                    )

            with pytest.raises(QueueIsEmpty):
                await queue.get_message("test-topic")

    async def test_topics(self):
        queue: Queue
        async with self.with_queue(["test1", "test2"]) as queue:
            await queue.put_message("test1", "one")
            await queue.put_message("test2", "two")
            await queue.put_message("test1", "one")
            assert (await queue.get_message("test2")).body == "two"
            assert (await queue.get_message("test1")).body == "one"
            assert (await queue.get_message("test1")).body == "one"
