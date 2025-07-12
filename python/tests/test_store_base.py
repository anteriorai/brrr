import functools

from brrr.backends.in_memory import InMemoryByteStore
from brrr.pickle_codec import PickleCodec
from brrr.store import (
    CompareMismatch,
    Memory,
    MemKey,
)


class FlakyStore(InMemoryByteStore):
    # fail every other CAS operation
    fail_cas: bool

    def __init__(self):
        super().__init__()
        self.fail_cas = True

    async def set_new_value(self, key: MemKey, value: bytes):
        self.fail_cas = not self.fail_cas
        if not self.fail_cas:
            raise CompareMismatch()
        return await super().set_new_value(key, value)

    async def compare_and_set(self, key: MemKey, value: bytes, expected: bytes):
        self.fail_cas = not self.fail_cas
        if not self.fail_cas:
            raise CompareMismatch()
        return await super().compare_and_set(key, value, expected)

    async def compare_and_delete(self, key: MemKey, expected: bytes):
        self.fail_cas = not self.fail_cas
        if not self.fail_cas:
            raise CompareMismatch()
        return await super().compare_and_delete(key, expected)


async def test_memory_cas():
    store = FlakyStore()
    memory = Memory(store, PickleCodec())
    key = MemKey("foo", "bar")
    # Testing a private method is technically a bit of an anti pattern.  Tbh I
    # don’t think the API for the store is correct to begin with and we should
    # probably just remove it entirely.  This primitive though seems broken and
    # I need to test it now without rewriting the entire store API.
    await memory._with_cas(functools.partial(store.set_new_value, key, b"123"))
    assert b"123" == await store.get(key)
    await memory._with_cas(
        functools.partial(store.compare_and_set, key, b"999", b"123")
    )
    assert b"999" == await store.get(key)
