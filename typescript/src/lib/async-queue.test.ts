import { beforeEach, mock, suite, test } from "node:test";
import { AsyncQueue } from "./async-queue.ts";
import {
  deepStrictEqual,
  doesNotThrow,
  ok,
  rejects,
  strictEqual,
  throws,
} from "node:assert/strict";
import { setTimeout } from "node:timers/promises";

await suite(import.meta.filename, async () => {
  let queue: AsyncQueue<number>;
  const mockFn = mock.fn();

  beforeEach(() => {
    queue = new AsyncQueue<number>();
    mockFn.mock.resetCalls();
  });

  await suite("basic", async () => {
    await test("basic push & pop", async () => {
      await queue.push(0);
      await queue.push(1);
      deepStrictEqual(await queue.pop(), {
        kind: "Ok",
        value: 0,
      });
      deepStrictEqual(await queue.pop(), {
        kind: "Ok",
        value: 1,
      });
    });

    await test("pop blocks until item is pushed", async () => {
      const pop = queue.pop().then(mockFn);
      strictEqual(mockFn.mock.callCount(), 0);
      await queue.push(0);
      await pop;
      strictEqual(mockFn.mock.callCount(), 1);
    });

    await test("popSync works when queue has items", async () => {
      await queue.push(0);
      const val = queue.popSync();
      deepStrictEqual(val, {
        kind: "Ok",
        value: 0,
      });
    });

    await test("popSync throws on empty queue", () => {
      strictEqual(queue.popSync().kind, "QueueIsEmpty");
    });

    await test("timeout is enforced on pop", async () => {
      const timeout = 50;
      const pop = queue.pop(timeout);
      await setTimeout(timeout * 2);
      await queue.push(0);
      strictEqual((await pop).kind, "QueueIsEmpty");
    });

    await test("timeout sanity check", async () => {
      const timeout = 50;
      const pop = queue.pop(timeout);
      await queue.push(0);
      strictEqual((await pop).kind, "Ok");
    });
  });

  await suite("shutdown", async () => {
    await test("pop throws if queue is shutdown and empty", async () => {
      queue.shutdown();
      strictEqual((await queue.pop()).kind, "QueueIsClosed");
    });

    await test("push throws if queue is shutdown", async () => {
      queue.shutdown();
      await rejects(queue.push(0), { message: "Queue is closed" });
    });

    await test("blocked pop rejects on shutdown", async () => {
      const pop = queue.pop();
      queue.shutdown();
      await rejects(pop, { message: "Queue is closed" });
    });

    await test("multiple blocked pops reject on shutdown", async () => {
      const a = queue.pop();
      const b = queue.pop();
      const c = queue.pop();
      queue.shutdown();
      await rejects(a, { message: "Queue is closed" });
      await rejects(b, { message: "Queue is closed" });
      await rejects(c, { message: "Queue is closed" });
    });

    await test("shutdown is idempotent", async () => {
      queue.shutdown();
      doesNotThrow(() => queue.shutdown());
      doesNotThrow(() => queue.shutdown());
      await rejects(() => queue.push(0), { message: "Queue is closed" });
      strictEqual((await queue.pop()).kind, "QueueIsClosed");
    });
  });

  await suite("task tracking", async () => {
    await test("taskDone and join work correctly", async () => {
      const values = [0, 1, 2];
      for (const value of values) {
        await queue.push(value);
      }
      strictEqual(queue.size(), values.length);
      for (const _ of values) {
        await queue.pop();
      }
      await queue.join();
      throws(() => queue.done());
    });

    await test("shutdown allows remaining tasks to be tracked", async () => {
      await queue.push(0);
      await queue.push(1);
      queue.shutdown();
      deepStrictEqual(await queue.pop(), {
        kind: "Ok",
        value: 0,
      });
      deepStrictEqual(await queue.pop(), {
        kind: "Ok",
        value: 1,
      });
      await queue.join();
    });

    await test("join resolves immediately when no tasks", async () => {
      await queue.join();
    });

    await test("join resolves after all tasks are done", async () => {
      await queue.push(0);
      await queue.push(1);

      const join = queue.join().then(mockFn);
      strictEqual(mockFn.mock.callCount(), 0);

      await queue.pop();
      await queue.pop();

      await join;
      strictEqual(mockFn.mock.callCount(), 1);
    });
  });

  await suite("simple stress test", async () => {
    await test("producer-consumer with shutdown", async () => {
      const values = [0, 1, 2, 3, 4];
      const promises: Promise<void>[] = [];
      for (const _ of values) {
        promises.push(
          queue.pop().then((val) => {
            mockFn(val);
            queue.done();
          }),
        );
      }
      for (const value of values) {
        promises.push(queue.push(value));
      }
      queue.shutdown();
      await Promise.allSettled(promises);
      deepStrictEqual(
        mockFn.mock.calls.flatMap((call) => call.arguments),
        values.map((value) => ({
          kind: "Ok",
          value,
        })),
      );
    });

    await test("rapid shutdown and restart", async () => {
      for (let i = 0; i < 5; i++) {
        const queue = new AsyncQueue<number>();
        const push = queue.push(i);
        const pop = queue.pop();
        queue.shutdown();
        await Promise.allSettled([push, pop]);
      }
    });

    await test("deferred cleanup on shutdown", async () => {
      const pops = new Array(10).keys().map(() => queue.pop());
      queue.shutdown();
      const results = await Promise.all(pops);
      ok(results.every((result) => result.kind === "QueueIsClosed"));
    });
  });
});
