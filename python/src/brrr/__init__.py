from .brrr import Brrr, Client as Client, Task as Task, Wrrrker as Wrrrker
from .brrr import Defer as Defer, DeferredCall as DeferredCall
from .brrr import SpawnLimitError as SpawnLimitError

# For ergonomics, we provide a singleton and a bunch of proxies as the module interface.
_brrr = Brrr()

connect = _brrr.connect
task = _brrr.task
worker = _brrr.worker
wrrrk = _brrr.wrrrk
