import dataclasses
from dataclasses import dataclass
from typing import Any, ClassVar, Self

import bencodepy

_bc = bencodepy.Bencode(encoding="utf-8")


@dataclass(frozen=True)
class TaggedTuple:
    """Ad-hoc static registry of record serialization semantics.

    Compare to protobuf but only semantically (the actual encoding/decoding is
    expected to be done by bencode) and far less featureful.

    The responsibility is on the developer to never re-use a version and always
    bump the version number when anything changes.

    The version is just meant as a stateless, light-weight encoding/decoding
    assert--not for any runtime type inference.

    The semantics are light weight and can be encoded using bencode using
    minimal overhead (which is why it's a tuple instead of a dict).

    These datastructures are _not_ part of the public brrr API, but they _are_
    part of the brrr wire protocol, meaning they must be supported by all
    languages implementing brrr.

    """

    tag: ClassVar[int]

    def astuple(self) -> tuple[Any, ...]:
        return (self.tag,) + dataclasses.astuple(self)

    @classmethod
    def fromtuple(cls, t: tuple[Any, ...]) -> Self:
        if t[0] != cls.tag:
            raise ValueError(f"{cls.__name__} decode tag mismatch: {t[0]} != {cls.tag}")
        return cls(*t[1:])


@dataclass(frozen=True)
class TaggedTupleStrings(TaggedTuple):
    def encode(self) -> bytes:
        x: bytes = _bc.encode(self.astuple())
        return x

    @classmethod
    def decode(cls, enc: bytes) -> Self:
        return cls.fromtuple(_bc.decode(enc))


@dataclass(frozen=True)
class PendingReturn(TaggedTuple):
    tag = 1
    root_id: str
    call_hash: str
    topic: str


@dataclass(frozen=True)
class ScheduleMessage(TaggedTupleStrings):
    tag = 2
    root_id: str
    call_hash: str
