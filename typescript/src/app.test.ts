import { beforeEach, suite, test } from "node:test";
import { InMemoryByteStore, InMemoryQueue } from "./backends/in-memory.ts";
import { rejects, strictEqual } from "node:assert";
import {
  type ActiveWorker,
  AppConsumer,
  AppWorker,
  type Handlers,
  taskify,
} from "./app.ts";
import { Server } from "./connection.ts";
import { NaiveCodec } from "./naive-codec.ts";
import { NotFoundError } from "./errors.ts";
import { LocalBrrr } from "./local-app.ts";
import { ok } from "node:assert/strict";

const topic = "brrr-test";
const subtopics = ["t1", "t2", "t3"];

await suite(import.meta.filename, async () => {
  const codec = new NaiveCodec();

  function bar(a: number) {
    return 456;
  }

  async function foo(app: ActiveWorker, a: number) {
    return (await app.call(bar, topic)(a + 1)) + 1;
  }

  const handlers: Handlers = {
    bar: taskify(bar),
    foo,
  };

  let store: InMemoryByteStore;
  let queue: InMemoryQueue;

  beforeEach(() => {
    store = new InMemoryByteStore();
    queue = new InMemoryQueue([topic, ...subtopics]);
    queue.flush();
  });

  await test(AppWorker.name, async () => {
    const server = new Server(queue, store, store);
    const app = new AppWorker(codec, server, handlers);
    await app.schedule("foo", topic)(122);
    await server.loop(topic, app.handle);
    strictEqual(await app.read("foo")(122), 457);
    strictEqual(await app.read(foo)(122), 457);
    strictEqual(await app.read("bar")(123), 456);
    strictEqual(await app.read(bar)(123), 456);
  });

  await test(AppConsumer.name, async () => {
    function foo(n: number) {
      return n * n;
    }

    const workerServer = new Server(queue, store, store);
    const appWorker = new AppWorker(codec, workerServer, {
      foo: taskify(foo),
    });
    await appWorker.schedule("foo", topic)(5);
    await workerServer.loop(topic, appWorker.handle);

    const appConsumer = new AppConsumer(codec, workerServer);

    strictEqual(await appConsumer.read("foo")(5), 25);
    strictEqual(await appConsumer.read(foo)(5), 25);
    await rejects(appConsumer.read("foo")(3), NotFoundError);
    await rejects(appConsumer.read(foo)(3), NotFoundError);
    await rejects(appConsumer.read("bar")(5), NotFoundError);
    await rejects(appConsumer.read(bar)(5), NotFoundError);
  });

  await test(LocalBrrr.name, async () => {
    const brrr = new LocalBrrr(topic, handlers, codec);
    strictEqual(await brrr.run(foo)(122), 457);
  });

  await suite("gather", async () => {
    async function callNestedGather(useBrrGather = true): Promise<string[]> {
      const calls: string[] = []

      function foo(a: number): number {
        calls.push(`foo(${a})`);
        return a * 2
      }

      function bar(a: number): number {
        calls.push(`bar(${a})`);
        return a - 1
      }

      async function notBrrrTask(app: ActiveWorker, a: number): Promise<number> {
        const b = await app.call(foo)(a)
        return app.call(bar)(b)
      }

      async function top(app: ActiveWorker, xs: number[]) {
        calls.push(`top(${xs})`)
        if (useBrrGather) {
          return app.gather(...xs.map(x => notBrrrTask(app, x)))
        }
        return Promise.all(
          xs.map(x => notBrrrTask(app, x))
        )
      }

      const localBrrr = new LocalBrrr(topic, {
        foo: taskify(foo),
        bar: taskify(bar),
        top
      }, codec)
      await localBrrr.run(top)([3, 4])
      return calls
    }

    await test("app gather", async () => {
      const brrrCalls = await callNestedGather()
      strictEqual(brrrCalls.filter(it => it.startsWith("top")).length, 5)
      const foo3 = brrrCalls.indexOf("foo(3)")
      const foo4 = brrrCalls.indexOf("foo(4)")
      const bar6 = brrrCalls.indexOf("bar(6)")
      const bar8 = brrrCalls.indexOf("bar(8)")
      ok(foo3 < bar6)
      ok(foo3 < bar8)
      ok(foo4 < bar6)
      ok(foo4 < bar8)
    })

    await test("Promise.all gather", async () => {
      const promises = await callNestedGather(false)
      strictEqual(promises.filter(it => it.startsWith("top")).length, 5)
      const foo3 = promises.indexOf("foo(3)")
      const foo4 = promises.indexOf("foo(4)")
      const bar6 = promises.indexOf("bar(6)")
      const bar8 = promises.indexOf("bar(8)")
      ok(foo3 < bar6)
      ok(foo4 < bar8)
    })
  })

  await test("test topics separate app same connection", async () => {
    function one(a: number): number {
      return a + 5
    }

    async function two(app: ActiveWorker, a: number): Promise<void> {
      const result = await app.call(one, "t1")(a + 3)
      strictEqual(result, 15)
      await queue.close()
    }

    const server = new Server(queue, store, store);
    const app1 = new AppWorker(codec, server, {
      one: taskify(one),
    })
    const app2 = new AppWorker(codec, server, { two, })
    await app2.schedule(two, "t2")(7);
    await Promise.all([
      await server.loop("t1", app1.handle),
      await server.loop("t2", app2.handle)
    ])
    await queue.join()
  })
});
