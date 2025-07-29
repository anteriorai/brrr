import brrr
from brrr.call import Call
from brrr import Connection, Defer, DeferredCall, Request, Response
from brrr.backends.in_memory import InMemoryByteStore, InMemoryQueue

TOPIC = "brrr-test"


async def test_conn_raw() -> None:
    store = InMemoryByteStore()
    queue = InMemoryQueue([TOPIC])

    async def handler(request: Request, conn: Connection) -> Defer | Response:
        call = request.call
        match call.task_name:
            case "inner":
                assert call.payload == b"inner call payload"
                return Response(payload=b"inner return value")
            case "foo":
                assert call.payload == b"123"
                response = await conn.read_raw("hash2")
                if response is None:
                    return Defer(
                        calls=[
                            DeferredCall(
                                topic=None,
                                call=Call(
                                    call_hash="hash2",
                                    task_name="inner",
                                    payload=b"inner call payload",
                                ),
                            ),
                        ]
                    )
                assert response == b"inner return value"
                return Response(payload=b"zim")
        assert False, f"Unknown task name: {call.task_name}"

    async with brrr.serve(queue, store, store) as conn:
        await conn.schedule_raw(TOPIC, "hash1", "foo", b"123")
        queue.flush()
        await conn.loop(TOPIC, handler)
        assert await conn.read_raw("hash1") == b"zim"
        assert await conn.read_raw("hash2") == b"inner return value"
        assert await conn.read_raw("hash3") is None


async def test_conn_nop_closed_queue() -> None:
    store = InMemoryByteStore()
    queue = InMemoryQueue([TOPIC])
    await queue.close()

    async def handler(request: Request, conn: Connection) -> Defer | Response:
        assert False

    async with brrr.serve(queue, store, store) as conn:
        await conn.loop(TOPIC, handler)
        await conn.loop(TOPIC, handler)
        await conn.loop(TOPIC, handler)
