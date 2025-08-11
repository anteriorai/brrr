import { beforeEach, mock, suite, test } from "node:test";
import { QueueIsClosedError, QueueIsEmptyError } from "../errors.ts";
import { AsyncQueue } from "./async-queue.ts";
import {
  deepStrictEqual,
  doesNotThrow,
  rejects,
  strictEqual,
  throws,
} from "node:assert";

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
      strictEqual(await queue.pop(), 0);
      strictEqual(await queue.pop(), 1);
    });

    await test("pop blocks until item is pushed", async (t) => {
      const pop = queue.pop().then(mockFn);
      strictEqual(mockFn.mock.callCount(), 0);
      await queue.push(0);
      await pop;
      strictEqual(mockFn.mock.callCount(), 1);
    });

    await test("popSync works when queue has items", async () => {
      await queue.push(0);
      const val = queue.popSync();
      strictEqual(val, 0);
    });

    await test("popSync throws on empty queue", () => {
      throws(() => queue.popSync(), QueueIsEmptyError);
    });
  });

  await suite("shutdown", async () => {
    await test("pop throws if queue is shutdown and empty", async () => {
      queue.shutdown();
      await rejects(queue.pop(), QueueIsClosedError);
    });

    await test("push throws if queue is shutdown", async () => {
      queue.shutdown();
      ok(!(await queue.push(0)));
    });

    await test("blocked pop rejects on shutdown", async () => {
      const pop = queue.pop();
      queue.shutdown();
      deepStrictEqual(await pop, { kind: "QueueIsClosed" });
    });

    await test("multiple blocked pops reject on shutdown", async () => {
      const a = queue.pop();
      const b = queue.pop();
      const c = queue.pop();
      queue.shutdown();
      deepStrictEqual(await a, { kind: "QueueIsClosed" });
      deepStrictEqual(await b, { kind: "QueueIsClosed" });
      deepStrictEqual(await c, { kind: "QueueIsClosed" });
    });

    await test("shutdown is idempotent", async () => {
      queue.shutdown();
      doesNotThrow(() => queue.shutdown());
      doesNotThrow(() => queue.shutdown());
      await rejects(() => queue.push(0), QueueIsClosedError);
      await rejects(() => queue.pop(), QueueIsClosedError);
    });
  });

  await suite("task tracking", async () => {
    await test("taskDone and join work correctly", async () => {
      const values = [0, 1, 2];
      for (const value of values) {
        await queue.push(value);
      }
      strictEqual(queue.size(), values.length);
      await queue.pop();
      doesNotThrow(() => queue.done());
      await queue.pop();
      doesNotThrow(() => queue.done());
      await queue.pop();
      doesNotThrow(() => queue.done());
      await queue.join();
      throws(() => queue.done());
    });

    await test("shutdown allows remaining tasks to be tracked", async () => {
      await queue.push(0);
      await queue.push(1);
      queue.shutdown();
      strictEqual(await queue.pop(), 0);
      queue.done();
      strictEqual(await queue.pop(), 1);
      queue.done();
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
      queue.done();
      await queue.pop();
      queue.done();

      strictEqual(mockFn.mock.callCount(), 0);
      await join;
      strictEqual(mockFn.mock.callCount(), 1);
    });
  });

  await suite("simple stress test", async () => {
    await test("producer-consumer with shutdown", async () => {
      const values = [0, 1, 2, 3, 4];
      const promises: Promise<boolean | void>[] = [];
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
        values,
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
  });
});
