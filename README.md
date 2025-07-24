# Brrr: high performance workflow scheduling

Brrr is a POC for ultra scalable workflow scheduling.

Differences between Brrr and other workflow schedulers:

- Brrr is **queue & database agnostic**. Others lock you in to e.g. PostgreSQL, which inevitably becomes an unscalable point of failure.
- Brrr **lets the queue & database provide stability & concurrency guarantees**.  Others tend to reinvent the wheel and reimplement a half-hearted queue on top of a database.  Brrr lets your queue and DB do what they do best.
- Brrr **requires idempotency** of the call graph.  It’s ok if your tasks return a different result per call, but the sub tasks they spin up must always be exactly the same, with the same inputs.
- Brrr tasks **look sequential & blocking**.  Your Python code looks like a simple linear function.
- Brrr tasks **aren’t actually blocking** which means you don’t need to lock up RAM in your fleet equivalent to the entire call graph’s execution stack.  In other words: A Brrr fleet’s memory usage is *O(fleet)*, not *O(call graph)*.
- Brrr offers **no logging, monitoring, error handling, or tracing**.  Brrr does one thing and one thing only: workflow scheduling.  Bring Your Own Logging.
- Brrr has **no agent**.  Every worker connects directly to the underlying queue, jobs are scheduled by directly sending them to the queue.  This allows *massive parallelism*: your only limit is your queue & DB capacity.
- Brrr makes **no encoding choices**: the only datatype seen by Brrr is "array of bytes".  You must Bring Your Own Encoder.

N.B.: That last point means that you can use Brrr with SQS & DynamoDB to scale basically as far as your wallet can stretch without any further config.

To summarize, these elements are not provided, and you must Bring Your Own:

- queue
- KV store
- logging
- tracing
- encoding

Brrr is a protocol that can be implemented in many languages. "It's just bytes on the wire."

## Development

There is currently only one SDK implementation: Python, async.

A Nix devshell is provided for Python which can be used for development and testing:

```
$ nix develop .#python
```

It uses [uv2nix](https://github.com/pyproject-nix/uv2nix) to parse the uv.lock file into a full-blown Nix managed development environment.

A generic Nix devshell is provided with some tools on the path but without uv2nix, for managing the Python packages or fixing uv if the lock file breaks somehow (e.g. git conflicts):

```
$ nix develop
```

## Python Library

Brrr is a dependency-free Python uv bundle which you can import and use directly.

Look at the [`brrr_demo.py`](brrr_demo.py) file for a full demo.

Highlights:

```py
import brrr

async def fib(app: brrr.ActiveWorker, n: int, salt=None):
    match n:
        case 0: return 0
        case 1: return 1
        case _: return sum(await app().gather(
            app.call(fib)(n - 2),
            app.call(fib)(n - 1),
        ))

async def fib_and_print(app: brrr.ActiveWorker, n: str):
    f = await app.call(fib)(int(n))
    print(f"fib({n}) = {f}", flush=True)
    return f

async def hello(greetee: str):
    greeting = f"Hello, {greetee}!"
    print(greeting, flush=True)
    return greeting

...
```

Note: the `.call(fib)` calls don’t ever actually block for the execution of the underlying logic: the entire parent function instead is aborted and re-executed multiple times until all its inputs are available.

Benefit: your code looks intuitive.

Drawback: the call graph must be idempotent, meaning: for the same inputs, a task must always call the same sub-tasks with the same arguments.  It is allowed to return a different result each time.


## Demo

Requires [Nix](https://nixos.org), with flakes enabled.

You can start the full demo without installation:

```
$ nix run github:nobssoftware/brrr#demo
```

In the process list, select the worker process so you can see its output.   Now in another terminal:

```
$ curl 'http://localhost:8333/hello?greetee=John'
```

You should see the worker print a greeting.

You can also run a Fibonacci job:

```
$ curl 'http://localhost:8333/fib_and_print?n=11'
```


## Copyright & License

Brrr was written by Robin Lewis and Jesse Zwaan.  It is available under the AGPLv3 license (not later).

See the [LICENSE](LICENSE) file.
