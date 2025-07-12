#!/usr/bin/env python3

import ast
import asyncio
from contextlib import asynccontextmanager
from collections.abc import AsyncIterator
import logging
import logging.config
import json
import os
from pprint import pprint
import sys
from typing import Any, Iterable

import aioboto3
from aiohttp import web
import redis.asyncio as redis
from types_aiobotocore_dynamodb import DynamoDBClient

from brrr.backends.redis import RedisQueue
from brrr.backends.dynamo import DynamoDbMemStore
import brrr
from brrr.naive_codec import PickleCodec

logger = logging.getLogger(__name__)
routes = web.RouteTableDef()


def table_name() -> str:
    """
    Get table name from environment
    """
    return os.environ.get("DYNAMODB_TABLE_NAME", "brrr")


def response(status: int, content: dict):
    return web.Response(status=status, text=json.dumps(content))


@routes.get("/{task_name}")
async def get_task_result(request: web.BaseRequest):
    # aiohttp uses a multidict but we don’t need that for this demo.
    kwargs = dict(request.query)

    task_name = request.match_info["task_name"]
    if task_name not in brrr.tasks:
        return response(404, {"error": "No such task"})

    try:
        result = await brrr.read(task_name, (), kwargs)
    except KeyError:
        return response(404, dict(error="No result for this task"))
    return response(200, dict(status="ok", result=result))


@routes.post("/{task_name}")
async def schedule_task(request: web.BaseRequest):
    kwargs = dict(request.query)

    task_name = request.match_info["task_name"]
    if task_name not in brrr.tasks:
        return response(404, {"error": "No such task"})

    await brrr.schedule("brrr-demo-main", task_name, (), kwargs)
    return response(202, {"status": "accepted"})


# ... where is the python contextmanager monad?


@asynccontextmanager
async def with_redis(
    redurl: str | None = os.environ.get("BRRR_DEMO_REDIS_URL"),
) -> AsyncIterator[redis.Redis]:
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


@asynccontextmanager
async def with_resources() -> AsyncIterator[tuple[redis.Redis, DynamoDBClient]]:
    async with with_redis() as rc:
        dync: DynamoDBClient
        async with aioboto3.Session().client("dynamodb") as dync:
            yield (rc, dync)


@asynccontextmanager
async def with_brrr_resources() -> AsyncIterator[tuple[RedisQueue, DynamoDbMemStore]]:
    async with with_resources() as (rc, dync):
        store = DynamoDbMemStore(dync, table_name())
        queue = RedisQueue(rc)
        yield (queue, store)


@asynccontextmanager
async def with_brrr(reset_backends):
    async with with_brrr_resources() as (redis, dynamo):
        if reset_backends:
            await redis.setup()
            await dynamo.create_table()
        brrr.setup(redis, dynamo, redis, PickleCodec())
        yield


# This is the normal way to initialize tasks: just directly use the module
# singleton.
@brrr.task
async def fib_and_print(n: str, salt=None):
    f = await brrr.call("brrr-demo-side", "fib", (int(n), salt), {})
    print(f"fib({n}) = {f}", flush=True)
    return f


@brrr.task
async def hello(greetee: str):
    greeting = f"Hello, {greetee}!"
    print(greeting, flush=True)
    return greeting


# In-line demo of separate topics.  Normally you would probably just have this
# in an entirely separate file, or even an entirely separate project.  But this
# works, too.
b2 = brrr.Brrr()


@b2.task
async def fib(n: int, salt=None):
    match n:
        case 0 | 1:
            return n
        case _:
            return sum(await brrr.gather(fib(n - 2, salt), fib(n - 1, salt)))


cmds = {}


def cmd(f):
    cmds[f.__name__] = f
    return f


@cmd
async def worker():
    async with with_brrr_resources() as (redis, dynamo):
        brrr.setup(redis, dynamo, redis, PickleCodec())
        # You can safely share the exact same resources between different brrr
        # instances, even if they use different topics.  In particular the
        # memory store is topic-agnostic.
        b2.setup(redis, dynamo, redis, PickleCodec())
        async with brrr.wrrrk() as client_main:
            async with b2.wrrrk() as client_side:
                await asyncio.gather(
                    client_main.loop("brrr-demo-main"),
                    client_side.loop("brrr-demo-side"),
                )


@cmd
async def server():
    bind_addr = os.environ.get("BRRR_DEMO_LISTEN_HOST", "127.0.0.1")
    bind_port = int(os.environ.get("BRRR_DEMO_LISTEN_PORT", "8080"))
    async with with_brrr(True):
        app = web.Application()
        app.add_routes(routes)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, bind_addr, bind_port)
        await site.start()
        logger.info(f"Listening on http://{bind_addr}:{bind_port}")
        await asyncio.Event().wait()


def args2dict(args: Iterable[str]) -> dict[str, Any]:
    """
    Extremely rudimentary arbitrary argparser.

    args2dict(["--foo", "bar", "--zim", "zom"])
    => {"foo": "bar", "zim": "zom"}

    """
    it = iter(args)

    # Best effort eval()
    def maybeval(x: str):
        try:
            return ast.literal_eval(x)
        except ValueError:
            return x

    return {k.lstrip("-"): maybeval(v) for k, v in zip(it, it)}


@cmd
async def schedule(topic: str, job: str, *args: str):
    """
    Put a single job onto the queue
    """
    async with with_brrr(False):
        await brrr.schedule(topic, job, (), args2dict(args))


@cmd
async def monitor():
    async with with_brrr_resources() as (queue, _):
        while True:
            pprint(await queue.get_info("brrr-demo-main"))
            await asyncio.sleep(1)


@cmd
async def reset():
    async with with_resources() as (rc, dync):
        try:
            await dync.delete_table(TableName=table_name())
        except Exception as e:
            # Table does not exist
            if "ResourceNotFoundException" not in str(e):
                raise

        await rc.flushall()


async def amain():
    # To log _all_ messages at DEBGUG level (very noisy)
    # logging.basicConfig(level=logging.DEBUG)
    logging.basicConfig()
    logger.setLevel(logging.DEBUG)
    # To log all brrr messages at DEBUG level (quite noisy)
    # logging.getLogger('brrr').setLevel(logging.DEBUG)
    f = cmds.get(sys.argv[1]) if len(sys.argv) > 1 else None
    if f:
        await f(*sys.argv[2:])
    else:
        print(f"Usage: brrr_demo.py <{' | '.join(cmds.keys())}>")
        sys.exit(1)


def main():
    try:
        asyncio.run(amain())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
