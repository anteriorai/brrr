import { beforeEach, describe, test } from "node:test";
import { deepStrictEqual, ok } from "node:assert/strict";
import {
  type Cache,
  type MemKey,
  Memory,
  PendingReturns,
  type Store,
} from "./store.ts";
import type { Queue } from "./queue.ts";
import { doesNotReject, rejects, strictEqual } from "node:assert";
import {
  CompareMismatchError,
  NotFoundError,
  QueueIsClosedError,
  UnknownTopicError,
} from "./errors.ts";

await describe(import.meta.filename, async () => {
  await describe(PendingReturns.name, async () => {
    await test("Encoded payload can be encoded & decoded", async () => {
      const original = new PendingReturns(0, [
        new PendingReturn("a", "b", "c"),
      ]);
      const encoded = original.encode();
      const decoded = PendingReturns.decode(encoded);
      deepStrictEqual(original, decoded);
      deepStrictEqual(encoded, decoded.encode());
    });

    await test("Encoded payload with undefined timestamp can be encoded & decoded", async () => {
      const original = new PendingReturns(undefined, [
        new PendingReturn("a", "b", "c"),
      ]);
      const encoded = original.encode();
      const decoded = PendingReturns.decode(encoded);
      deepStrictEqual(original, decoded);
      deepStrictEqual(encoded, decoded.encode());
    });
  });
});

export async function storeContractTest(factory: () => Store) {
  await describe("store-contract", async () => {
    let store: Store;

    const fixture = {
      key: {
        type: "call",
        callHash: "test-call-hash",
      } satisfies MemKey,
      value: new Uint8Array([1, 2, 3, 4, 5]),
      otherKey: {
        type: "call",
        callHash: "other-test-call-hash",
      },
    } as const;

    beforeEach(async () => {
      store = factory();
      await store.set(fixture.key, fixture.value);
    });

    await test("Basic get", async () => {
      const retrieved = await store.get(fixture.key);
      deepStrictEqual(retrieved, fixture.value);
      strictEqual(await store.get(fixture.otherKey), undefined);
    });

    await test("Basic has", async () => {
      ok(await store.has(fixture.key));
      ok(!(await store.has(fixture.otherKey)));
    });

    await test("Basic set", async () => {
      const newKey: MemKey = {
        type: "call",
        callHash: "new-call-hash",
      };
      const newValue = new Uint8Array([6, 7, 8, 9, 10]);
      await store.set(newKey, newValue);
      const retrieved = await store.get(newKey);
      deepStrictEqual(retrieved, newValue);
    });

    await test("Basic delete", async () => {
      await store.delete(fixture.key);
      strictEqual(await store.get(fixture.key), undefined);
      strictEqual(await store.delete(fixture.otherKey), false);
    });

    await test("Basic setNewValue", async () => {
      const newValue = new Uint8Array([6, 7, 8, 9, 10]);
      ok(!(await store.setNewValue(fixture.key, newValue)));
      await doesNotReject(store.setNewValue(fixture.otherKey, newValue));
      const retrieved = await store.get(fixture.otherKey);
      deepStrictEqual(retrieved, newValue);
    });

    await test("Basic compareAndSet", async () => {
      const newValue = new Uint8Array([6, 7, 8, 9, 10]);
      await store.compareAndSet(fixture.key, newValue, fixture.value);
      const retrieved = await store.get(fixture.key);
      deepStrictEqual(retrieved, newValue);
      ok(
        !(await store.compareAndSet(fixture.otherKey, newValue, fixture.value)),
      );
    });

    await test("Basic compareAndDelete", async () => {
      await store.compareAndDelete(fixture.key, fixture.value);
      strictEqual(await store.get(fixture.key), undefined);
      ok(!(await store.compareAndDelete(fixture.otherKey, fixture.value)));
    });
  });
}

export async function cacheContractTest(factory: () => Cache) {
  await describe("cache-contract", async () => {
    let cache: Cache;

    beforeEach(() => {
      cache = factory();
    });

    await test("Basic incr", async () => {
      const key = "test-incr-key";
      const initialValue = await cache.incr(key);
      strictEqual(initialValue, 1);
      const nextValue = await cache.incr(key);
      strictEqual(nextValue, 2);
    });
  });
}

export async function queueContractTest(factory: (topics: string[]) => Queue) {
  await describe("queue-contract", async () => {
    let queue: Queue;

    const mockFn = mock.fn();
    const fixture = {
      topic: "test-topic",
      message: {
        body: "test-message",
      } satisfies Message,
    } as const;

    beforeEach(() => {
      queue = factory([fixture.topic]);
      queue.put(fixture.topic, fixture.message);
    });

    await test("Basic get", async () => {
      strictEqual(await queue.get(fixture.topic), fixture.message);
    });

    await test("Basic put", async () => {
      const newMessage = "new-test-message";
      await queue.put(fixture.topic, newMessage);
      strictEqual(await queue.get(fixture.topic), fixture.message);
      strictEqual(await queue.get(fixture.topic), newMessage);
    });

    await test("Non-existing topic operations should throw", async () => {
      await rejects(queue.get("non-existing-topic"), UnknownTopicError);
      await rejects(
        queue.put("non-existing-topic", "message"),
        UnknownTopicError,
      );
    });

    await test("Queue can be closed", async () => {
      await doesNotReject(queue.close());
      await rejects(queue.close(), QueueIsClosedError);
      await rejects(queue.get(fixture.topic), QueueIsClosedError);
      await rejects(queue.put(fixture.topic, "message"), QueueIsClosedError);
    });
  });
}
