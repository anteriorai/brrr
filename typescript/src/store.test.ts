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
  UnknownTopicError,
} from "./errors.ts";
import { InMemoryByteStore } from "./backends/in-memory.ts";
import { Call } from "./call.ts";

await describe(import.meta.filename, async () => {
  await describe(PendingReturns.name, async () => {
    await test("Encoded payload can be encoded & decoded", async () => {
      const original = new PendingReturns(0, new Set(["a", "b", "c"]));
      const encoded = original.encode();
      const decoded = PendingReturns.decode(encoded);
      deepStrictEqual(original, decoded);
      deepStrictEqual(encoded, decoded.encode());
    });

    await test("Encoded payload with undefined timestamp can be encoded & decoded", async () => {
      const original = new PendingReturns(undefined, new Set(["a", "b", "c"]));
      const encoded = original.encode();
      const decoded = PendingReturns.decode(encoded);
      deepStrictEqual(original, decoded);
      deepStrictEqual(encoded, decoded.encode());
    });
  });

  await describe(Memory.name, async () => {
    let store: Store;
    let memory: Memory;

    const fixture = {
      call: new Call("test-task", new Uint8Array([1, 2, 3]), "test-call-hash"),
    } as const;

    beforeEach(async () => {
      store = new InMemoryByteStore();
      memory = new Memory(store);
      await memory.setCall(fixture.call);
      await memory.setValue(fixture.call.callHash, fixture.call.payload);
    });

    await test("getCall", async () => {
      const retrieved = await memory.getCall(fixture.call.callHash);
      ok(retrieved.equals(fixture.call));
    });

    await test("setCall", async () => {
      const newCall = new Call(
        "new-task",
        new Uint8Array([4, 5, 6]),
        "new-call-hash",
      );
      await memory.setCall(newCall);
      const retrieved = await memory.getCall(newCall.callHash);
      ok(retrieved.equals(newCall));
    });

    await test("hasValue", async () => {
      ok(await memory.hasValue(fixture.call.callHash));
      ok(!(await memory.hasValue("non-existing-call-hash")));
    });

    await test("getValue", async () => {
      const retrieved = await memory.getValue(fixture.call.callHash);
      deepStrictEqual(retrieved, fixture.call.payload);
      await rejects(memory.getValue("non-existing-call-hash"), NotFoundError);
    });

    await test("setValue", async () => {
      const newPayload = new Uint8Array([7, 8, 9]);
      await memory.setValue(fixture.call.callHash, newPayload);
      const retrieved = await memory.getValue(fixture.call.callHash);
      deepStrictEqual(retrieved, newPayload);
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
      await rejects(store.get(fixture.otherKey), NotFoundError);
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
      await rejects(store.get(fixture.key), NotFoundError);
      await rejects(store.delete(fixture.otherKey), NotFoundError);
    });

    await test("Basic setNewValue", async () => {
      const newValue = new Uint8Array([6, 7, 8, 9, 10]);
      await rejects(
        store.setNewValue(fixture.key, newValue),
        CompareMismatchError,
      );
      await doesNotReject(store.setNewValue(fixture.otherKey, newValue));
      const retrieved = await store.get(fixture.otherKey);
      deepStrictEqual(retrieved, newValue);
    });

    await test("Basic compareAndSet", async () => {
      const newValue = new Uint8Array([6, 7, 8, 9, 10]);
      await store.compareAndSet(fixture.key, newValue, fixture.value);
      const retrieved = await store.get(fixture.key);
      deepStrictEqual(retrieved, newValue);
      await rejects(
        store.compareAndSet(fixture.otherKey, newValue, fixture.value),
        CompareMismatchError,
      );
    });

    await test("Basic compareAndDelete", async () => {
      await store.compareAndDelete(fixture.key, fixture.value);
      await rejects(store.get(fixture.key), NotFoundError);
      await rejects(
        store.compareAndDelete(fixture.otherKey, fixture.value),
        CompareMismatchError,
      );
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

    const fixture = {
      topic: "test-topic",
      message: "test-message",
    } as const;

    beforeEach(() => {
      queue = factory([fixture.topic]);
      queue.push(fixture.topic, fixture.message);
    });

    await test("Basic get", async () => {
      strictEqual(await queue.pop(fixture.topic), fixture.message);
    });

    await test("Basic push", async () => {
      const newMessage = "new-test-message";
      await queue.push(fixture.topic, newMessage);
      strictEqual(await queue.pop(fixture.topic), fixture.message);
      strictEqual(await queue.pop(fixture.topic), newMessage);
    });

    await test("Non-existing topic operations should throw", async () => {
      await rejects(queue.pop("non-existing-topic"), UnknownTopicError);
      await rejects(
        queue.push("non-existing-topic", "message"),
        UnknownTopicError,
      );
    });
  });
}
