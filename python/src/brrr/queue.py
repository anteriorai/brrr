from abc import ABC, abstractmethod
from dataclasses import dataclass


class QueueIsEmpty(Exception):
    pass


class QueueIsClosed(Exception):
    """
    Queue implementations may raise this exception for workers to shut down gracefully
    """


@dataclass
class Message:
    """Pointless future proof wrapper around getting a queue message.

    YAGNI and all but something tells me this will be necessary again soon.

    """

    body: str


@dataclass
class QueueInfo:
    """
    Approximate info about a queue.

    Vestigial and purely  best effort at this point.
    """

    num_messages: int


# Infra abstractions


class Queue(ABC):
    # Inspired by SQS: maximum time for a get_message call to block while
    # waiting for new messages, if there are currently no messages on the queue.
    # Just as with SQS, if there is a message, return immediately.  This value
    # is best effort, just a way to keep this DRY if nothing else.  If the
    # underlying queue doesn’t support this primitive it’s OK, but don’t sleep
    # for this time indiscriminately because the point of this value is to block
    # only while there are no messages available.
    recv_block_secs: int = 20

    throws_queue_is_closed: bool
    has_accurate_info: bool
    deletes_messages: bool

    @abstractmethod
    async def put_message(self, topic: str, body: str) -> None: ...
    @abstractmethod
    async def get_message(self, topic: str) -> Message: ...
