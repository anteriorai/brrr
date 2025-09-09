from __future__ import annotations

import asyncio
import logging
import time
import typing

import bencodepy
import bencodepy.exceptions
from redis.exceptions import (
    ConnectionError as RedisConnectionError,
    ResponseError as RedisResponseError,
)

from ..queue import Message, Queue, QueueInfo, QueueIsEmpty
from ..store import Cache

if typing.TYPE_CHECKING:
    from redis.asyncio import Redis


logger = logging.getLogger(__name__)

WARNING_LOG_INTERVAL = 5
RETRY_TIMEOUT_SECS = 60.0


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

    def _is_retryable(self, e: BaseException) -> bool:
        """
        Retry only for the two transient errors we have seen in production: -
        RedisConnectionError: connection dropped by server while blocking -
        RedisResponseError: server-side UNBLOCK of a blocking operation

        Intentionally keeping this narrow to avoid masking logic errors.
        -po 2025-09-09
        """
        return isinstance(e, (RedisConnectionError, RedisResponseError))

    async def _force_disconnect(self) -> None:
        # Best-effort: force the pool to reconnect on next command.
        #
        # Potential risk: this resets all connections in the client's pool. If
        # the same client instance is shared between producers and consumers
        # within the same process (as in anterior/platform/ with a single client
        # for queue+cache), they may see a brief reconnect latency spike.
        # redis-py auto-reconnects for new commands, so correctness is
        # preserved. -po 2025-09-09
        pool = getattr(self.client, "connection_pool", None)
        if pool is not None:
            try:
                coro = pool.disconnect()  # type: ignore[no-untyped-call]
                if asyncio.iscoroutine(coro):
                    await coro
            except Exception:
                pass

    async def get_message(self, topic: str) -> Message:
        # Simple retry loop for transient disconnects/failovers with a fixed
        # small delay. We cap retries by time to avoid infinite waits on
        # misconfiguration.
        sleep_secs = 0.2
        attempts = 0
        deadline = time.monotonic() + RETRY_TIMEOUT_SECS  # hard cap
        last_error: BaseException | None = None
        while True:
            try:
                response = await self.client.blpop(topic, self.recv_block_secs)
                if not response:
                    raise QueueIsEmpty()
                try:
                    chunks = bencodepy.decode(response[1])
                except bencodepy.exceptions.BencodeDecodeError:
                    logger.error(
                        f"Invalid bencoded message in queue {topic}: {repr(response)}"
                    )
                    raise
                if chunks[0] != 1:
                    # The message has disappeared from the queue and won’t be retried
                    # anyway.
                    logger.error(
                        f"Invalid message structure in {topic}: {repr(chunks)}"
                    )
                    raise ValueError("Invalid message in queue")
                # Ignore timestamp for now
                return Message(chunks[2].decode("utf-8"))
            except Exception as e:
                if not self._is_retryable(e):
                    raise
                attempts += 1
                last_error = e
                # simple rate-limit for warnings to reduce log noise in a
                # failover storm
                if attempts == 1 or attempts % WARNING_LOG_INTERVAL == 0:
                    logger.warning(
                        "RedisQueue.get_message retrying after transient error",
                        exc_info=e,
                    )
                else:
                    logger.debug(
                        "RedisQueue.get_message transient error; will retry",
                        exc_info=False,
                    )
                await self._force_disconnect()
                if time.monotonic() >= deadline:
                    # give up and surface the last error
                    raise last_error  # type: ignore[misc]
                await asyncio.sleep(sleep_secs)

    async def get_info(self, topic: str):
        total = await self.client.llen(topic)
        return QueueInfo(num_messages=total)

    async def incr(self, key: str) -> int:
        return await self.client.incr(key)
