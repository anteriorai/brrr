import asyncio
from collections.abc import Sequence


import brrr.queue as bqueue


class ClosableInMemQueue(bqueue.Queue):
    """A message queue which can be closed."""

    def __init__(self, topics: Sequence[str]):
        self.closing = False
        self.queues = {k: asyncio.Queue() for k in topics}

    async def close(self):
        assert not self.closing
        self.closing = True
        for q in self.queues.values():
            q.shutdown()

    async def join(self):
        await asyncio.gather(*(q.join() for q in self.queues.values()))

    async def get_message(self, topic: str):
        if topic not in self.queues:
            raise ValueError("invalid topic name")

        q = self.queues[topic]
        try:
            payload = await q.get()
        except asyncio.QueueShutDown:
            raise bqueue.QueueIsClosed()

        q.task_done()
        return bqueue.Message(body=payload)

    async def put_message(self, topic: str, body: str):
        await self.queues[topic].put(body)
