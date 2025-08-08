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

const topic = "brrr-test";

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
    queue = new InMemoryQueue([topic]);
  });

  await test(AppWorker.name, async () => {
    const server = new Server(queue, store, store);
    const app = new AppWorker(codec, server, handlers);
    await app.schedule("foo", topic)(122);
    queue.flush();
    await server.loop(topic, app.handle);
    strictEqual(await app.read("foo")(122), 457);
    strictEqual(await app.read("bar")(123), 456);
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

    queue.flush();
    await workerServer.loop(topic, appWorker.handle);

    const appConsumer = new AppConsumer(codec, workerServer);

    strictEqual(await appConsumer.read("foo")(5), 25);
    await rejects(appConsumer.read("foo")(3), NotFoundError);
    await rejects(appConsumer.read("bar")(5), NotFoundError);
  });

  await test("", async () => {
    const brrr = new LocalBrrr(topic, handlers, codec);
    strictEqual(await brrr.run(foo)(122), 457);
  });
});
