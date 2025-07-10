from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from brrr.backends.in_memory import InMemoryByteStore
from brrr.store import Store

from .contract_store import MemoryContract


class TestInMemoryByteStore(MemoryContract):
    @asynccontextmanager
    async def with_store(self) -> AsyncIterator[Store]:
        yield InMemoryByteStore()
