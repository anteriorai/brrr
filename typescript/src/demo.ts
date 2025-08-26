import { InMemoryCache, InMemoryStore } from "./backends/in-memory.ts";
import { type Handlers, taskFn } from "./app";
import { EventEmitter } from "node:events";

type Containers = {
  store: InMemoryStore,
  cache: InMemoryCache
}

function foo(n: number) {
  return n + 1
}

class Brrr extends EventEmitter {
  readonly containers: Containers
  readonly handlers: Handlers

  constructor(containers: Containers, handlers: Handlers) {
    super();
    this.containers = containers;
    this.handlers = handlers;
  }

  schedule(name: string, topic: string) {
    return async (...args: unknown[]) => {
      // NOCOOMIT
      this.emit(topic, /* callhash */);
    }
  }

  listen(topic: string) {
    this.on()
  }
}

////

const store = new InMemoryStore()
const cache = new InMemoryCache()
const handlers: Handlers = {
  foo: taskFn(foo)
}

const brrrScheduler = new Brrr({ store, cache }, handlers)
await brrrScheduler.schedule("foo", "topic")(1)

const brrrWorker = new Brrr({ store, cache }, handlers)
brrrWorker.
