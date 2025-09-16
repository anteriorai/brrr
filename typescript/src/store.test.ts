import {
  afterEach,
  before,
  beforeEach,
  mock,
  type MockTimersOptions,
  suite,
  test,
} from "node:test";
import {
  deepStrictEqual,
  doesNotReject,
  ok,
  strictEqual,
} from "node:assert/strict";
import {
  type Cache,
  type MemKey,
  Memory,
  PendingReturns,
  type Store,
} from "./store.ts";
import { InMemoryStore } from "./backends/in-memory.ts";
import type { Call } from "./call.ts";
import { PendingReturn, taggedTuple } from "./tagged-tuple.ts";

await suite(import.meta.filename, async () => {
  await suite(PendingReturns.name, async () => {
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

  await suite(Memory.name, async () => {
    let store: Store;
    let memory: Memory;

    const fixture = {
      call: {
        taskName: "test-task",
        payload: new Uint8Array([1, 2, 3]),
        callHash: "test-call-hash",
      } satisfies Call,
      pendingReturns: {
        key: {
          type: "pending_returns",
          callHash: "test-pending-return-hash",
        } satisfies MemKey,
      },
      newReturn: new PendingReturn("some-root", "some-parent", "some-topic"),
    } as const;

    beforeEach(async () => {
      store = new InMemoryStore();
      memory = new Memory(store);
      await memory.setCall(fixture.call);
      await memory.setValue(fixture.call.callHash, fixture.call.payload);
    });

    await test("getCall", async () => {
      const retrieved = await memory.getCall(fixture.call.callHash);
      deepStrictEqual(retrieved, fixture.call);
    });

    await test("setCall", async () => {
      const newCall: Call = {
        taskName: "new-task",
        payload: new Uint8Array([4, 5, 6]),
        callHash: "new-call-hash",
      };
      await memory.setCall(newCall);
      const retrieved = await memory.getCall(newCall.callHash);
      deepStrictEqual(retrieved, newCall);
    });

    await test("hasValue", async () => {
      ok(await memory.hasValue(fixture.call.callHash));
      ok(!(await memory.hasValue("non-existing-call-hash")));
    });

    await test("getValue", async () => {
      const retrieved = await memory.getValue(fixture.call.callHash);
      deepStrictEqual(retrieved, fixture.call.payload);
      strictEqual(await memory.getValue("non-existing-call-hash"), undefined);
    });

    await test("setValue", async () => {
      const newPayload = new Uint8Array([7, 8, 9]);
      await memory.setValue(fixture.call.callHash, newPayload);
      const retrieved = await memory.getValue(fixture.call.callHash);
      deepStrictEqual(retrieved, newPayload);
    });

    await suite("addPendingReturn", async () => {
      const mockTimersOptions = {
        apis: ["Date"],
        now: 5000,
      } as const satisfies MockTimersOptions;

      before(() => {
        mock.timers.enable(mockTimersOptions);
      });

      await test("simple cases to document & test shouldSchedule", async () => {
        const hash = "some-hash";
        const base = new PendingReturn("root", "parent", "topic");

        const cases = [
          // base case
          [hash, base, true],
          // same one, shouldn't schedule again
          [hash, base, false],
          // different root, should schedule - it's a retry
          [hash, new PendingReturn("diff-root", "parent", "topic"), true],
          // new callHash, new PR, should schedule
          ["diff-hash", base, true],
          // continuation, shouldn't schedule again
          [hash, new PendingReturn("root", "parent", "diff-topic"), false],
          [hash, new PendingReturn("root", "diff-parent", "topic"), false],
          [hash, new PendingReturn("root", "diff-parent", "diff-topic"), false],
        ] as const;

        for (const [hash, pr, shouldSchedule] of cases) {
          strictEqual(await memory.addPendingReturns(hash, pr), shouldSchedule);
        }

        // ensure all returns are stored
        const encoded = await store.get({
          type: "pending_returns",
          callHash: hash,
        });
        deepStrictEqual(
          PendingReturns.decode(encoded!).encodedReturns,
          new Set(cases.map((it) => taggedTuple.encodeToString(it[1]))),
        );
      });

      await test("First-time call triggers schedule and stores return", async () => {
        const shouldSchedule = await memory.addPendingReturns(
          fixture.call.callHash,
          fixture.newReturn,
        );
        ok(shouldSchedule);
        const raw = await store.get({
          type: "pending_returns",
          callHash: fixture.call.callHash,
        });
        ok(raw);
        const decoded = PendingReturns.decode(raw);
        ok(decoded.encodedReturns.has(taggedTuple.encodeToString(fixture.newReturn)));
        strictEqual(decoded.scheduledAt, mockTimersOptions.now / 1000);
      });

      await test("Repeated call with same return does not call schedule again", async () => {
        await memory.addPendingReturns(
          fixture.call.callHash,
          fixture.newReturn,
        );
        const shouldSchedule = await memory.addPendingReturns(
          fixture.call.callHash,
          fixture.newReturn,
        );
        ok(!shouldSchedule);
        const raw = await store.get({
          type: "pending_returns",
          callHash: fixture.call.callHash,
        });
        ok(raw);
        const decoded = PendingReturns.decode(raw);
        deepStrictEqual(
          decoded.encodedReturns,
          new Set([taggedTuple.encodeToString(fixture.newReturn)]),
        );
      });

      await test("Handles different returns properly", async () => {
        await memory.addPendingReturns(
          fixture.call.callHash,
          fixture.newReturn,
        );
        const completelyDifferentReturn = new PendingReturn(
          "completely",
          "different",
          "return",
        );
        const shouldSchedule = await memory.addPendingReturns(
          fixture.call.callHash,
          completelyDifferentReturn,
        );
        ok(!shouldSchedule);
        const raw = await store.get({
          type: "pending_returns",
          callHash: fixture.call.callHash,
        });
        ok(raw);
        const decoded = PendingReturns.decode(raw);
        deepStrictEqual(
          decoded.encodedReturns,
          new Set(
            [fixture.newReturn, completelyDifferentReturn].map((it) =>
              taggedTuple.encodeToString(it),
            ),
          ),
        );
      });

      await test("Repeated call with different rootId should schedule again", async () => {
        const returnWithDifferentRoot = new PendingReturn(
          "other-root",
          fixture.newReturn.callHash,
          fixture.newReturn.topic,
        );
        const shouldSchedule = await memory.addPendingReturns(
          fixture.call.callHash,
          returnWithDifferentRoot,
        );
        ok(shouldSchedule);
        const raw = await store.get({
          type: "pending_returns",
          callHash: fixture.call.callHash,
        });
        ok(raw);
        const decoded = PendingReturns.decode(raw);
        ok(
          decoded.encodedReturns.has(taggedTuple.encodeToString(returnWithDifferentRoot)),
        );
        strictEqual(decoded.scheduledAt, mockTimersOptions.now / 1000);
      });
    });

    await suite("withPendingReturnRemove", async () => {
      const mockFn =
        mock.fn<(returns: Iterable<PendingReturn>) => Promise<void>>();

      afterEach(() => {
        mockFn.mock.resetCalls();
      });

      await test("don't call f if no pending return is found", async () => {
        await memory.withPendingReturnsRemove(fixture.call.callHash, mockFn);
        strictEqual(mockFn.mock.callCount(), 0);
      });

      await test("invokes f with pending returns and deletes the key", async () => {
        const pendingReturns = new PendingReturns(undefined, [
          new PendingReturn("a", "b", "c"),
          new PendingReturn("d", "e", "f"),
        ]);
        await store.set(fixture.pendingReturns.key, pendingReturns.encode());
        await memory.withPendingReturnsRemove(
          fixture.pendingReturns.key.callHash,
          (returns) => {
            deepStrictEqual(
              [...returns].map((it) => taggedTuple.encodeToString(it)),
              [...pendingReturns.encodedReturns],
            );
            return mockFn(returns);
          },
        );
        strictEqual(mockFn.mock.callCount(), 1);
        strictEqual(await store.get(fixture.pendingReturns.key), undefined);
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
