from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
import functools

from .app import AppWorker, WrappedTask
from .backends.in_memory import InMemoryByteStore, InMemoryQueue
from .connection import Server, serve
from .codec import Codec


class LocalApp:
    def __init__(
        self, *, topic: str, conn: Server, queue: InMemoryQueue, app: AppWorker
    ) -> None:
        self._conn = conn
        self._app = app
        self._queue = queue
        self._topic = topic
        self.schedule = functools.partial(app.schedule, topic=topic)
        self.read = app.read
        self._has_run = False

    async def run(self) -> None:
        if self._has_run:
            raise ValueError("LocalApp has already run")
        self._has_run = True
        self._queue.flush()
        await self._conn.loop(self._topic, self._app.handle)


@asynccontextmanager
async def local_app(
    topic: str, handlers: Mapping[str, WrappedTask], codec: Codec
) -> AsyncIterator[LocalApp]:
    """
    Helper function for unit tests which use brrr
    """
    store = InMemoryByteStore()
    queue = InMemoryQueue([topic])

    async with serve(queue, store, store) as conn:
        app = AppWorker(handlers=handlers, codec=codec, connection=conn)
        yield LocalApp(topic=topic, conn=conn, queue=queue, app=app)
