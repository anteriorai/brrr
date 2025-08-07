import { beforeEach, suite, test } from "node:test";
import type { Cache, Store } from "./store.ts";
import type { Queue } from "./queue.ts";
import { InMemoryByteStore, InMemoryQueue } from "./backends/in-memory.ts";
import { rejects, strictEqual } from "node:assert";
import { type ActiveWorker, AppWorker, handlerify } from "./app.ts";
import { connect } from "./connection.ts";
import { NaiveCodec } from "./naive-codec.ts";
import { stringify } from "node:querystring";

const topic = "brrr-test";

await suite(import.meta.filename, async () => {
  const codec = new NaiveCodec();

  let store: InMemoryByteStore;
  let queue: InMemoryQueue;

  beforeEach(() => {
    store = new InMemoryByteStore();
    queue = new InMemoryQueue([topic]);
  });

  await test("App type test", async () => {
    function foo(n: number, s: string) {
      return n;
    }

    function bar(app: ActiveWorker, n: number) {
      return n;
    }

    async function baz(app: ActiveWorker, n: number) {
      await app.call(bar, topic)(n);
      await app.call(foo, topic)(n, n.toString());
      // @ts-expect-error
      await app.call(foo, topic)(n);
      // @ts-expect-error
      await app.call(foo, topic)(n, n.toString(), n);
      // @ts-expect-error
      await app.call(bar, topic)(app, n);
      return n;
    }
  });

  await test("App worker", async () => {
    function bar(a: number) {
      strictEqual(a, 123);
      return 456;
    }

    async function foo(app: ActiveWorker, a: number) {
      return (await app.call(bar, topic)(a + 1)) + 1;
    }

    await using connection = await connect(queue, store, store);
    const app = new AppWorker(codec, connection, {
      bar: handlerify(bar),
      foo,
    });
    await app.schedule(foo, topic)(122);
    await queue.flush();
    const loop = connection.loop(topic, app.handle);
    await queue.close();
    await loop
  });

  // await test("App consumer", async () => {
  //
  //   function foo(n: number) {
  //     return n * n;
  //   }
  //
  //   function _foo(n: number, app: ActiveSOrker) {
  //
  //   }
  //
  //
  //     await using workerConnection = await connect(queue, store, store);
  //   const appWorker = new AppWorker(codec,  workerConnection, { foo });
  //   await appWorker.schedule(foo, { topic })(5);
  //   await workerConnection.loop(topic, appWorker.handle)
  //
  //     await using consumerConnection = await connect(queue, store, store);
  //   strictEqual(await consumerConnection.read("foo")(5), 25)
  //   await rejects(consumerConnection.read("foo")(3), NotFoundError)
  //   await rejects(consumerConnection.read("bar")(5), NotFoundError)
  // });
});
