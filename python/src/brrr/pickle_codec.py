from dataclasses import dataclass
import hashlib
import pickle
from typing import Any

from brrr.call import Call
from brrr.codec import Codec


@dataclass
class ArgsKwargsCall(Call):
    """
    Primitive catch-all call implementation without nuance.
    """

    args: tuple
    kwargs: dict

    def __str__(self):
        return f"Call({self.task_name}, {', '.join(self.args)}, {', '.join(map(lambda x: '%s=%r' % x, self.kwargs.items()))}"


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

    def create_call(self, task_name: str, args: tuple, kwargs: dict) -> ArgsKwargsCall:
        memo_key = self._hash_call(task_name, args, kwargs)
        return ArgsKwargsCall(
            task_name=task_name, args=args, kwargs=kwargs, memo_key=memo_key
        )

    def encode_call(self, call: Call) -> bytes:
        if not isinstance(call, ArgsKwargsCall):
            raise ValueError("encode_call only accept ArgsKwargsCall")
        return pickle.dumps((call.args, call.kwargs))

    async def invoke_task(
        self, memo_key: str, name: str, handler, payload: bytes
    ) -> bytes:
        args, kwargs = pickle.loads(payload)
        return pickle.dumps(await handler(*args, **kwargs))

    def decode_return(self, payload: bytes) -> Any:
        return pickle.loads(payload)
