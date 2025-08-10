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
import { deepStrictEqual, ok } from "node:assert/strict";

const codec = new NaiveCodec();
const topic = "brrr-test";
const subtopics = {
  t1: "t1",
  t2: "t2",
  t3: "t3",
} as const;

let store: InMemoryByteStore;
let queue: InMemoryQueue;
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
  const result = await app.call(one, subtopics.t1)(a + 3);
  strictEqual(result, 15);
  await queue.close();
}

const handlers: Handlers = {
  bar: taskify(bar),
  foo,
};

await suite(import.meta.filename, async () => {
  beforeEach(() => {
    store = new InMemoryByteStore();
    queue = new InMemoryQueue([topic, ...Object.values(subtopics)]);
    server = new Server(queue, store, store);
    queue.flush();
  });

  await test(AppWorker.name, async () => {
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
          foo: taskify(foo),
          bar: taskify(bar),
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
      one: taskify(one),
    });
    const app2 = new AppWorker(codec, server, { two });
    await app2.schedule(two, "t2")(7);
    await Promise.all([
      await server.loop(subtopics.t1, app1.handle),
      await server.loop("t2", app2.handle),
    ]);
    await queue.join();
  });

  await test("topics separate app separate connection", async () => {
    const server1 = new Server(queue, store, store);
    const server2 = new Server(queue, store, store);
    const app1 = new AppWorker(codec, server1, {
      one: taskify(one),
    });
    const app2 = new AppWorker(codec, server2, { two });
    await app2.schedule(two, subtopics.t2)(7);
    await Promise.all([
      server1.loop(subtopics.t1, app1.handle),
      server2.loop(subtopics.t2, app2.handle),
    ]);
    await queue.join();
  });

  await test("topics same app", async () => {
    const app = new AppWorker(codec, server, {
      one: taskify(one),
      two,
    });
    await app.schedule(two, subtopics.t2)(7);
    await Promise.all([
      server.loop(subtopics.t1, app.handle),
      server.loop(subtopics.t2, app.handle),
    ]);
    await queue.join();
  });

  await test("app nop closed queue", async () => {
    const app = new AppWorker(codec, server, {});
    await queue.close();
    await server.loop(topic, app.handle);
    await server.loop(topic, app.handle);
    await server.loop(topic, app.handle);
  });

  await test("test stop when empty", async () => {
    const pre = new Map<number, number>();
    const post = new Map<number, number>();

    async function foo(app: ActiveWorker, a: number): Promise<number> {
      pre.set(a, (pre.get(a) || 0) + 1);
      if (a === 0) {
        return 0;
      }
      const result = await app.call(foo)(a - 1);
      post.set(a, (post.get(a) || 0) + 1);
      return result;
    }

    const app = new AppWorker(codec, server, { foo });
    await app.schedule(foo, topic)(3);
    await server.loop(topic, app.handle);
    await queue.join();

    deepStrictEqual(Object.fromEntries(pre), { 0: 1, 1: 2, 2: 2, 3: 2 });
    deepStrictEqual(Object.fromEntries(post), { 1: 1, 2: 1, 3: 1 });
  });

  await test("test parallel", async () => {
    const parallel = 5;
    let barrier: Promise<void> | undefined = Promise.resolve();
    let tops = 0;

    async function block(a: number): Promise<number> {
      if (barrier) {
        await barrier;
      }
      barrier = undefined;
      return a;
    }

    async function top(app: ActiveWorker): Promise<void> {
      await app.gather(
        ...new Array(parallel).keys().map((i) => app.call(block)(i)),
      );
      tops++;
      if (tops === parallel) {
        await queue.close();
      }
    }

    const app = new AppWorker(codec, server, {
      block: taskify(block),
      top,
    });
    await app.schedule(top, topic)();
    await Promise.all(
      new Array(parallel)
        .keys()
        .map(() => server.loop(topic, app.handle)),
    );
    await queue.join();
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

    await Promise.all(new Array(10)
      .keys()
      .map(() => server.loop(topic, app.handle))
    );
    await queue.join();
  });

  await test("test debounce child", { only: true }, async () => {
    const calls = new Map<number, number>();

    async function foo(app: ActiveWorker, a: number): Promise<number> {
      calls.set(a, (calls.get(a) || 0) + 1);
      if (a === 0) {
        return a;
      }
      const results = await app.gather(
        ...Array(50)
          .keys()
          .map(() => app.call(foo)(a - 1))
      );
      return results.reduce((sum, val) => sum + val, 0);
    }

    const brrr = new LocalBrrr(topic, { foo }, codec);
    await brrr.run(foo)(3);

    deepStrictEqual(Object.fromEntries(calls), { 0: 1, 1: 2, 2: 2, 3: 2 })
  });

  // await test("test no debounce parent", async () => {
  //   const calls = new Map<string, number>();
  //
  //   function one(_: number): number {
  //     calls.set("one", (calls.get("one") || 0) + 1);
  //     return 1;
  //   }
  //
  //   async function foo(app: ActiveWorker, a: number): Promise<number> {
  //     calls.set("foo", (calls.get("foo") || 0) + 1);
  //     const results = await app.gather(
  //       ...Array.from({ length: a }, (_, i) => app.call(one)(i))
  //     );
  //     return results.reduce((sum: number, val) => sum + (val as number), 0);
  //   }
  //
  //   const brrr = new LocalBrrr(topic, {
  //     one: taskify(one),
  //     foo,
  //   }, codec);
  //   await brrr.run(foo as any)(50);
  //
  //   strictEqual(calls.get("one")!, 50);
  //   strictEqual(calls.get("foo")!, 51);
  // })
  //
  // await test("test app loop resumable", async () => {
  //   let errors = 5;
  //
  //   class MyError extends Error {
  //     constructor(message: string) {
  //       super(message);
  //       this.name = "MyError";
  //     }
  //   }
  //
  //   async function foo(a: number): Promise<number> {
  //     if (errors > 0) {
  //       errors -= 1;
  //       throw new MyError("retry");
  //     }
  //     await queue.close();
  //     return a;
  //   }
  //
  //   const app = new AppWorker(codec, server, { foo: taskify(foo) });
  //
  //   while (true) {
  //     try {
  //       await app.schedule(foo, topic)(3);
  //       await server.loop(topic, app.handle);
  //       break;
  //     } catch (err) {
  //       if (err instanceof MyError) {
  //         continue;
  //       }
  //       throw err;
  //     }
  //   }
  //
  //   await queue.join();
  //   strictEqual(errors, 0);
  // })
  //
  // await test("test app handler names", async () => {
  //   function foo(a: number): number {
  //     return a * a;
  //   }
  //
  //   async function bar(app: ActiveWorker, a: number): Promise<number> {
  //     return await app.call(foo)(a) * (await app.call("quux/zim")(a) as number);
  //   }
  //
  //   const handlers = {
  //     "quux/zim": taskify(foo),
  //     "quux/bar": bar,
  //   };
  //
  //   const worker = new AppWorker(codec, server, handlers)
  //   const localApp = new LocalApp(topic, server, queue, worker)
  //   await localApp.schedule("quux/bar")(4);
  //   await localApp.run()
  //   strictEqual(await localApp.read("quux/zim")(4), 16);
  //   // strictEqual(await localApp.read(foo)(4), 16);
  // })
  //
  // await test("test app subclass", async () => {
  //   function bar(a: number): number {
  //     return a + 1;
  //   }
  //
  //   function baz(a: number): number {
  //     return a + 10;
  //   }
  //
  //   async function foo(app: ActiveWorker, a: number): Promise<number> {
  //     return app.call(bar)(a);
  //   }
  //
  //   const app = new AppWorker(codec, server, {
  //     foo,
  //     bar: taskify(bar),
  //     baz: taskify(baz),
  //   });
  //
  //   await app.schedule(foo, topic)(4);
  //   await server.loop(topic, app.handle);
  //   strictEqual(await app.read(foo)(4), 5);
  // });
});
