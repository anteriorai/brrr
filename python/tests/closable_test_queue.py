import asyncio
import collections


import brrr.queue as bqueue


_CloseSentinel = object()


class ClosableInMemQueue(bqueue.Queue):
    """A message queue which can be closed."""

    def __init__(self):
        self.closing = False
        self.received = collections.defaultdict(asyncio.Queue)

    async def close(self):
        assert not self.closing
        self.closing = True
        for q in self.received.values():
            q.shutdown()

    async def join(self):
        await asyncio.gather(*(q.join() for q in self.received.values()))

    async def get_message(self, topic: str):
        if self.closing and topic not in self.received:
            raise bqueue.QueueIsClosed()

        q = self.received[topic]
        try:
            payload = await q.get()
        except asyncio.QueueShutDown:
            del self.received[topic]
            raise bqueue.QueueIsClosed()

        q.task_done()
        return bqueue.Message(body=payload)

    async def put_message(self, topic: str, body: str):
        await self.received[topic].put(body)
