from __future__ import annotations

import logging
import time
import typing

import bencodepy

from ..queue import Message, Queue, QueueInfo, QueueIsEmpty
from ..store import Cache

if typing.TYPE_CHECKING:
    from redis.asyncio import Redis


logger = logging.getLogger(__name__)


class RedisQueue(Queue, Cache):
    client: Redis
    queue: str

    def __init__(self, client: Redis, queue: str):
        self.client = client
        self.queue = queue

    async def setup(self):
        pass

    async def put_message(self, body: str):
        logger.debug(f"Putting new message on {self.queue}")
        val = bencodepy.encode((1, int(time.time()), body.encode("utf-8")))
        await self.client.rpush(self.queue, val)

    async def get_message(self) -> Message:
        response = await self.client.blpop(self.queue, self.recv_block_secs)
        if not response:
            raise QueueIsEmpty()
        try:
            chunks = bencodepy.decode(response[1])
        except bencodepy.exceptions.BencodeDecodeError:
            logger.error(
                f"Invalid bencoded message in queue {self.queue}: {repr(response)}"
            )
            raise
        if chunks[0] != 1:
            # The message has disappeared from the queue and won’t be retried
            # anyway.
            logger.error(f"Invalid message structure in {self.queue}: {repr(chunks)}")
            raise ValueError("Invalid message in queue")
        # Ignore timestamp for now
        return Message(chunks[2].decode("utf-8"))

    async def get_info(self):
        total = await self.client.llen(self.queue)
        return QueueInfo(num_messages=total)

    async def incr(self, key: str) -> int:
        return await self.client.incr(key)
