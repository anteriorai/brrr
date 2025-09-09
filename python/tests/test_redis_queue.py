import os
import time
from collections.abc import Sequence
from contextlib import asynccontextmanager
from typing import AsyncIterator

import bencodepy
import brrr.backends.redis as redis_backend_module
import pytest
import redis.asyncio as redis
from brrr.backends.redis import RETRY_TIMEOUT_SECS, RedisQueue
from brrr.queue import Queue
from redis.exceptions import (
    ConnectionError as RedisConnectionError,
)
from redis.exceptions import (
    ResponseError as RedisResponseError,
)
from redis.exceptions import DataError

from tests.contract_queue import QueueContract


@asynccontextmanager
async def with_redis(redurl: str | None) -> AsyncIterator[redis.Redis]:
    rkwargs = dict(
        decode_responses=True,
        health_check_interval=10,
        socket_connect_timeout=5,
        retry_on_timeout=True,
        socket_keepalive=True,
        protocol=3,
    )
    if redurl is None:
        rc = redis.Redis(**rkwargs)
    else:
        rc = redis.from_url(redurl, **rkwargs)

    await rc.ping()
    try:
        yield rc
    finally:
        await rc.aclose()


@pytest.mark.dependencies
class TestRedisQueue(QueueContract):
    has_accurate_info = True

    @asynccontextmanager
    async def with_queue(self, topics: Sequence[str]) -> AsyncIterator[Queue]:
        # Hack but worth it for testing
        RedisQueue.recv_block_secs = 1
        async with with_redis(os.environ.get("BRRR_TEST_REDIS_URL")) as rc:
            yield RedisQueue(rc)

    async def test_decode_error(self):
        async with self.with_queue(["test"]) as queue:
            assert isinstance(queue, RedisQueue)
            await queue.client.rpush("test", b"wrong")
            with pytest.raises(Exception):
                assert await queue.get_message("test")

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "exc_cls,context",
        [
            (RedisResponseError, "server UNBLOCK during BLPOP (failover/role change)"),
            (RedisConnectionError, "socket drop during BLPOP"),
        ],
    )
    async def test_transient_retry_then_success(self, exc_cls, context):
        topic = "test"
        text = "ok"
        body = text.encode()
        version = 1
        async with self.with_queue([topic]) as queue:
            assert isinstance(queue, RedisQueue)

            # simulate a single transient failure on the first BLPOP (either a
            # ResponseError for UNBLOCK or a ConnectionError for a dropped
            # socket), then ensure get_message retries and returns the enqueued
            # message
            original_blpop = queue.client.blpop

            async def one_shot_error(_key, _timeout):
                queue.client.blpop = original_blpop
                raise exc_cls(f"transient: {context}")

            queue.client.blpop = one_shot_error
            payload = bencodepy.encode((version, int(time.time()), body))
            await queue.client.rpush(topic, payload)
            msg = await queue.get_message(topic)
            assert msg.body == text

    @pytest.mark.asyncio
    async def test_hard_cap_raises_last_error(self, monkeypatch):
        topic = "test"
        async with self.with_queue([topic]) as queue:
            assert isinstance(queue, RedisQueue)

            async def always_fail(_key, _timeout):
                raise RedisConnectionError("this will always fail")

            queue.client.blpop = always_fail

            # HACK: fast-forward the internal monotonic clock to exceed the
            # RETRY_TIMEOUT_SECS cap
            t = {"n": 0}

            def fast_monotonic():
                # the first call sets the baseline; the second call exceeds the
                # deadline
                val = t["n"]
                t["n"] = RETRY_TIMEOUT_SECS + 1.0
                return val

            monkeypatch.setattr(redis_backend_module.time, "monotonic", fast_monotonic)

            with pytest.raises(RedisConnectionError):
                await queue.get_message(topic)

    @pytest.mark.asyncio
    async def test_non_retryable_dataerror_raises_immediately(self):
        topic = "test"
        async with self.with_queue([topic]) as queue:
            assert isinstance(queue, RedisQueue)

            # test that an error other than ConnectionError or ResponseError
            # raises immediately instead of retrying
            async def immediate_dataerror(_key, _timeout):
                raise DataError("arbitrary dataerror error")

            queue.client.blpop = immediate_dataerror
            with pytest.raises(DataError):
                await queue.get_message(topic)
