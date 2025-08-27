import { beforeEach, suite, test } from "node:test";
import { strictEqual } from "node:assert";
import {
  type ActiveWorker,
  AppConsumer,
  AppWorker,
  type Handlers,
  taskFn,
} from "./app.ts";
import { Server } from "./connection.ts";
import { InMemoryCache, InMemoryStore } from "./backends/in-memory.ts";
import { NaiveJsonCodec } from "./naive-json-codec.ts";
import type { Call } from "./call.ts";
import { NotFoundError } from "./errors.ts";
import { deepStrictEqual, ok, rejects } from "node:assert/strict";
import { LocalApp, LocalBrrr } from "./local-app.ts";
import type { Cache, Store } from "./store.ts";
import { EventEmitter } from "node:events";
import type { Emitter } from "./emitter.ts";

const codec = new NaiveJsonCodec();
const topic = "brrr-test";
const subtopics = {
  t1: "t1",
  t2: "t2",
  t3: "t3",
} as const;

let store: Store;
let cache: Cache;
let emitter: Emitter;
let server: Server;

// Test tasks
function bar(a: number) {
  return 456;
}

async function foo(app: ActiveWorker, a: number) {
  return (await app.call(bar, topic)(a + 1)) + 1;
}

function one(a: number): number {
  return a + 5;
}

async function two(app: ActiveWorker, a: number): Promise<void> {
  const result = await app.call("one", subtopics.t1)(a + 3);
  strictEqual(result, 15);
}

const handlers: Handlers = {
  bar: taskFn(bar),
  foo,
};

await suite(import.meta.filename, async () => {
  function waitForDone(
    emitter: AppConsumer,
    call: Call,
    predicate?: () => void | Promise<void>,
  ): Promise<void> {
    return new Promise((resolve) => {
      emitter.on("done", async ({ callHash }: Call) => {
        if (callHash === call.callHash) {
          await predicate?.();
          resolve();
        }
      });
    });
  }

  beforeEach(() => {
    store = new InMemoryStore();
    cache = new InMemoryCache();
    emitter = new EventEmitter();
    server = new Server(store, cache, emitter);
  });

  await test(AppWorker.name, async () => {
    const app = new AppWorker(codec, server, handlers);
    server.listen(topic, app.handle);

    const call = await codec.encodeCall(foo.name, [122]);

    const done = waitForDone(app, call, async () => {
      strictEqual(await app.read(foo)(122), 457);
      strictEqual(await app.read("foo")(122), 457);
      strictEqual(await app.read(foo)(122), 457);
      strictEqual(await app.read("bar")(123), 456);
      strictEqual(await app.read(bar)(123), 456);
    });

    await app.schedule(foo, topic)(122);
    return done;
  });

  await test(AppConsumer.name, async () => {
    function foo(n: number) {
      return n * n;
    }

    const workerServer = new Server(store, cache, emitter);
    const appWorker = new AppWorker(codec, workerServer, {
      foo: taskFn(foo),
    });
    workerServer.listen(topic, appWorker.handle);

    const appConsumer = new AppConsumer(codec, workerServer);
    const call = await codec.encodeCall(foo.name, [5]);

    const done = waitForDone(appConsumer, call, async () => {
      strictEqual(await appConsumer.read("foo")(5), 25);
      await rejects(appConsumer.read("foo")(3), NotFoundError);
      await rejects(appConsumer.read("bar")(5), NotFoundError);
    });

    await appWorker.schedule("foo", topic)(5);
    return done;
  });

  await test(LocalBrrr.name, async () => {
    const brrr = new LocalBrrr(topic, handlers, codec);
    strictEqual(await brrr.run(foo)(122), 457);
  });

  await suite("gather", async () => {
    async function callNestedGather(useBrrGather = true): Promise<string[]> {
      const calls: string[] = [];

      function foo(a: number): number {
        calls.push(`foo(${a})`);
        return a * 2;
      }

      function bar(a: number): number {
        calls.push(`bar(${a})`);
        return a - 1;
      }

      async function notBrrrTask(
        app: ActiveWorker,
        a: number,
      ): Promise<number> {
        const b = await app.call(foo)(a);
        return app.call(bar)(b);
      }

      async function top(app: ActiveWorker, xs: number[]) {
        calls.push(`top(${xs})`);
        if (useBrrGather) {
          return app.gather(...xs.map((x) => notBrrrTask(app, x)));
        }
        return Promise.all(xs.map((x) => notBrrrTask(app, x)));
      }

      const localBrrr = new LocalBrrr(
        topic,
        {
          foo: taskFn(foo),
          bar: taskFn(bar),
          top,
        },
        codec,
      );
      await localBrrr.run(top)([3, 4]);
      return calls;
    }

    await test("app gather", async () => {
      const brrrCalls = await callNestedGather();
      strictEqual(brrrCalls.filter((it) => it.startsWith("top")).length, 5);
      const foo3 = brrrCalls.indexOf("foo(3)");
      const foo4 = brrrCalls.indexOf("foo(4)");
      const bar6 = brrrCalls.indexOf("bar(6)");
      const bar8 = brrrCalls.indexOf("bar(8)");
      ok(foo3 < bar6);
      ok(foo3 < bar8);
      ok(foo4 < bar6);
      ok(foo4 < bar8);
    });

    await test("Promise.all gather", async () => {
      const promises = await callNestedGather(false);
      strictEqual(promises.filter((it) => it.startsWith("top")).length, 5);
      const foo3 = promises.indexOf("foo(3)");
      const foo4 = promises.indexOf("foo(4)");
      const bar6 = promises.indexOf("bar(6)");
      const bar8 = promises.indexOf("bar(8)");
      ok(foo3 < bar6);
      ok(foo4 < bar8);
    });
  });

  await test("topics separate app same connection", async () => {
    const app1 = new AppWorker(codec, server, {
      one: taskFn(one),
    });
    const app2 = new AppWorker(codec, server, { two });

    const call = await codec.encodeCall("two", [7]);

    const done = waitForDone(app2, call);

    server.listen(subtopics.t1, app1.handle);
    server.listen(subtopics.t2, app2.handle);

    await app2.schedule(two, subtopics.t2)(7);

    return done;
  });

  await test("topics separate app separate connection", async () => {
    const server1 = new Server(store, cache, emitter);
    const server2 = new Server(store, cache, emitter);
    const app1 = new AppWorker(codec, server1, {
      one: taskFn(one),
    });
    const app2 = new AppWorker(codec, server2, { two });

    server1.listen(subtopics.t1, app1.handle);
    server2.listen(subtopics.t2, app2.handle);

    const call = await codec.encodeCall("two", [7]);

    const done = waitForDone(app2, call);

    await app2.schedule(two, subtopics.t2)(7);
    return done;
  });

  await test("topics same app", async () => {
    const app = new AppWorker(codec, server, {
      one: taskFn(one),
      two,
    });
    server.listen(subtopics.t1, app.handle);
    server.listen(subtopics.t2, app.handle);
    await app.schedule(two, subtopics.t2)(7);
  });

  await test("stress parallel", async () => {
    async function fib(app: ActiveWorker, n: bigint): Promise<bigint> {
      if (n < 2) {
        return n;
      }
      const [a, b] = await app.gather(
        app.call(fib)(n - 1n),
        app.call(fib)(n - 2n),
      );
      return a + b;
    }

    async function top(app: ActiveWorker): Promise<void> {
      const n = await app.call(fib)(1000n);
      deepStrictEqual(
        n,
        43466557686937456435688527675040625802564660517371780402481729089536555417949051890403879840079255169295922593080322634775209689623239873322471161642996440906533187938298969649928516003704476137795166849228875n,
      );
    }

    const app = new AppWorker(codec, server, { fib, top });
    await app.schedule(top, topic)();

    await Promise.all(
      new Array(10).keys().map(() => server.listen(topic, app.handle)),
    );
  });

  await test("debounce child", async () => {
    const calls = new Map<number, number>();

    async function foo(app: ActiveWorker, a: number): Promise<number> {
      calls.set(a, (calls.get(a) || 0) + 1);
      if (a === 0) {
        return a;
      }
      const results = await app.gather(
        ...Array(50)
          .keys()
          .map(() => app.call(foo)(a - 1)),
      );
      return results.reduce((sum, val) => sum + val);
    }

    const brrr = new LocalBrrr(topic, { foo }, codec);
    await brrr.run(foo)(3);

    deepStrictEqual(Object.fromEntries(calls), { 0: 1, 1: 2, 2: 2, 3: 2 });
  });

  await test("no debounce parent", async () => {
    const calls = new Map<string, number>();

    function one(_: number): number {
      calls.set("one", (calls.get("one") || 0) + 1);
      return 1;
    }

    async function foo(app: ActiveWorker, a: number): Promise<number> {
      calls.set("foo", (calls.get("foo") || 0) + 1);
      const results = await app.gather(
        ...new Array(a).keys().map((i) => app.call(one)(i)),
      );
      return results.reduce((sum, val) => sum + val);
    }

    const brrr = new LocalBrrr(
      topic,
      {
        one: taskFn(one),
        foo,
      },
      codec,
    );
    await brrr.run(foo)(50);

    deepStrictEqual(Object.fromEntries(calls), { one: 50, foo: 51 });
  });

  await test("app handler names", async () => {
    function foo(a: number): number {
      return a * a;
    }

    async function bar(app: ActiveWorker, a: number): Promise<number> {
      return (
        (await app.call(foo)(a)) *
        (await app.call<[number], number>("quux/zim")(a))
      );
    }

    const worker = new AppWorker(codec, server, {
      "quux/zim": taskFn(foo),
      "quux/bar": bar,
    });
    const localApp = new LocalApp(topic, server, worker);
    localApp.run();

    const call = await codec.encodeCall("quux/bar", [4]);
    const done = waitForDone(localApp.app, call, async () => {
      strictEqual(await localApp.read("quux/zim")(4), 16);
      strictEqual(await localApp.read(foo)(4), 16);
    });

    await localApp.schedule("quux/bar")(4);
    return done;
  });
});
