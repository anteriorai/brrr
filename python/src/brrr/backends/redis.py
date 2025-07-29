from __future__ import annotations

import logging
import time
import typing

import bencodepy
import bencodepy.exceptions

from ..queue import Message, Queue, QueueInfo, QueueIsEmpty
from ..store import Cache

if typing.TYPE_CHECKING:
    from redis.asyncio import Redis


logger = logging.getLogger(__name__)


class RedisQueue(Queue, Cache):
    client: Redis

    def __init__(self, client: Redis):
        self.client = client

    async def setup(self):
        pass

    async def put_message(self, topic: str, body: str):
        logger.debug(f"Putting new message on {topic}")
        val = bencodepy.encode((1, int(time.time()), body.encode("utf-8")))
        await self.client.rpush(topic, val)

    async def get_message(self, topic: str) -> Message:
        response = await self.client.blpop(topic, self.recv_block_secs)
        if not response:
            raise QueueIsEmpty()
        try:
            chunks = bencodepy.decode(response[1])
        except bencodepy.exceptions.BencodeDecodeError:
            logger.error(f"Invalid bencoded message in queue {topic}: {repr(response)}")
            raise
        if chunks[0] != 1:
            # The message has disappeared from the queue and won’t be retried
            # anyway.
            logger.error(f"Invalid message structure in {topic}: {repr(chunks)}")
            raise ValueError("Invalid message in queue")
        # Ignore timestamp for now
        return Message(chunks[2].decode("utf-8"))

    async def get_info(self, topic: str):
        total = await self.client.llen(topic)
        return QueueInfo(num_messages=total)

    async def incr(self, key: str) -> int:
        return await self.client.incr(key)
