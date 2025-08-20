import hashlib
import pickle
from typing import Any, Callable

from .call import Call
from .codec import Codec


class PickleCodec(Codec):
    """Very liberal codec, based on hopes and dreams.

    Don't use this in production because you run the risk of non-deterministic
    serialization, e.g. dicts with arbitrary order.

    The primary purpose of this codec is executable documentation.

    """

    def _hash_call(self, task_name: str, args: tuple, kwargs: dict) -> str:
        h = hashlib.new("sha256")
        h.update(repr([task_name, args, list(sorted(kwargs.items()))]).encode())
        return h.hexdigest()

    def encode_call(self, task_name: str, args: tuple, kwargs: dict) -> Call:
        payload = pickle.dumps((args, kwargs))
        call_hash = self._hash_call(task_name, args, kwargs)
        return Call(task_name=task_name, payload=payload, call_hash=call_hash)

    async def invoke_task(self, call: Call, task: Callable) -> bytes:
        args, kwargs = pickle.loads(call.payload)
        return pickle.dumps(await task(*args, **kwargs))

    def decode_return(self, task_name: str, payload: bytes) -> Any:
        return pickle.loads(payload)
