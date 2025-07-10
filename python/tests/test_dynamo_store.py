import asyncio
from contextlib import asynccontextmanager
from typing import AsyncIterator
import uuid

import aioboto3
import pytest

from brrr.backends.dynamo import DynamoDbMemStore
from brrr.store import Store

from .contract_store import MemoryContract


@pytest.mark.dependencies
class TestDynamoByteStore(MemoryContract):
    @asynccontextmanager
    async def with_store(self) -> AsyncIterator[Store]:
        async with aioboto3.Session().client("dynamodb") as dync:
            table_name = f"brrr_test_{uuid.uuid4().hex}"
            memory = DynamoDbMemStore(dync, table_name)
            await memory.create_table()
            yield memory

    async def read_after_write_barrier(self):
        # Totally random guess that dynamo is generally RAW-consistent after
        # about 300ms.
        await asyncio.sleep(0.3)
