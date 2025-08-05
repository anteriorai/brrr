import {
  afterEach,
  before,
  beforeEach,
  describe,
  mock,
  type MockTimersOptions,
  suite,
  test,
} from "node:test";
import { deepStrictEqual, ok } from "node:assert/strict";
import {
  type Cache,
  type MemKey,
  Memory,
  PendingReturn,
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

await suite(import.meta.filename, async () => {
  await suite(PendingReturn.name, async () => {
    await test("Encoded payload can be encoded & decoded", async () => {
      const original = new PendingReturn(0, new Set(["a", "b", "c"]));
      const encoded = original.encode();
      const decoded = PendingReturn.decode(encoded);
      deepStrictEqual(original, decoded);
      deepStrictEqual(encoded, decoded.encode());
    });

    await test("Encoded payload with undefined timestamp can be encoded & decoded", async () => {
      const original = new PendingReturn(undefined, new Set(["a", "b", "c"]));
      const encoded = original.encode();
      const decoded = PendingReturn.decode(encoded);
      deepStrictEqual(original, decoded);
      deepStrictEqual(encoded, decoded.encode());
    });
  });

  await suite(Memory.name, async () => {
    let store: Store;
    let memory: Memory;

    const fixture = {
      call: new Call("test-task", new Uint8Array([1, 2, 3]), "test-call-hash"),
      pendingReturn: {
        key: {
          type: "pending_return",
          callHash: "test-pending-return-hash",
        } satisfies MemKey,
      },
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

    await describe("addPendingReturn", async () => {
      const mockFn = mock.fn<() => Promise<void>>();
      const mockTimersOptions = {
        apis: ["Date"],
        now: 5000,
      } as const satisfies MockTimersOptions;

      before(() => {
        mock.timers.enable(mockTimersOptions);
      });

      afterEach(() => {
        mockFn.mock.resetCalls();
      });

      await test("First-time call triggers schedule and stores return", async () => {
        const alreadyPending = await memory.addPendingReturn(
          fixture.call.callHash,
          "foo",
          mockFn,
        );
        ok(!alreadyPending);
        const raw = await store.get({
          type: "pending_return",
          callHash: fixture.call.callHash,
        });
        const decoded = PendingReturn.decode(raw);
        ok(decoded.returns.has("foo"));
        strictEqual(decoded.scheduledAt, mockTimersOptions.now / 1000);
        strictEqual(mockFn.mock.callCount(), 1);
      });

      await test("Repeated call with same return does not call schedule again", async () => {
        await memory.addPendingReturn(fixture.call.callHash, "foo", mockFn);
        const alreadyPending = await memory.addPendingReturn(
          fixture.call.callHash,
          "foo",
          mockFn,
        );
        ok(alreadyPending);
        strictEqual(mockFn.mock.callCount(), 1);
        const raw = await store.get({
          type: "pending_return",
          callHash: fixture.call.callHash,
        });
        const decoded = PendingReturn.decode(raw);
        deepStrictEqual(decoded.returns, new Set(["foo"]));
      });

      await test("Handles different returns properly", async () => {
        await memory.addPendingReturn(fixture.call.callHash, "foo", mockFn);
        const alreadyPending = await memory.addPendingReturn(
          fixture.call.callHash,
          "bar",
          mockFn,
        );
        ok(alreadyPending);
        const raw = await store.get({
          type: "pending_return",
          callHash: fixture.call.callHash,
        });
        const decoded = PendingReturn.decode(raw);
        deepStrictEqual(decoded.returns, new Set(["foo", "bar"]));
      });

      await test("Handles NotFoundError case correctly", async () => {
        const key: MemKey = {
          type: "pending_return",
          callHash: fixture.call.callHash,
        };
        await rejects(store.get(key), NotFoundError);
        const alreadyPending = await memory.addPendingReturn(
          fixture.call.callHash,
          "new-return",
          mockFn,
        );
        ok(!alreadyPending);
        const raw = await store.get(key);
        const decoded = PendingReturn.decode(raw);
        deepStrictEqual(decoded.returns, new Set(["new-return"]));
        strictEqual(decoded.scheduledAt, mockTimersOptions.now / 1000);
      });
    });

    await describe("withPendingReturnRemove", async () => {
      const mockFn = mock.fn<(returns: Iterable<string>) => Promise<void>>();

      afterEach(() => {
        mockFn.mock.resetCalls();
      });

      await test("calls f([]) if no pending return is found", async () => {
        await memory.withPendingReturnRemove(fixture.call.callHash, mockFn);
        strictEqual(mockFn.mock.callCount(), 1);
        deepStrictEqual(mockFn.mock.calls?.at(0)?.arguments, [new Set()]);
      });

      await test("invokes f with pending returns and deletes the key", async () => {
        const pendingReturn = new PendingReturn(undefined, new Set(["a", "b"]));
        await store.set(fixture.pendingReturn.key, pendingReturn.encode());
        await memory.withPendingReturnRemove(
          fixture.pendingReturn.key.callHash,
          mockFn,
        );
        strictEqual(mockFn.mock.callCount(), 1);
        deepStrictEqual(mockFn.mock.calls?.at(0)?.arguments, [
          pendingReturn.returns,
        ]);
        await rejects(store.get(fixture.pendingReturn.key), NotFoundError);
      });
    });
  });
});

export async function storeContractTest(factory: () => Store) {
  await suite("store-contract", async () => {
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
  await suite("cache-contract", async () => {
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
  await suite("queue-contract", async () => {
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
      queue.push(fixture.topic, fixture.message);
    });

    await test("Basic pop", async () => {
      strictEqual(await queue.pop(fixture.topic), fixture.message);
    });

    await test("Basic push & pop", async () => {
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
