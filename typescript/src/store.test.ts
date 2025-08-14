import { describe, test } from "node:test";
import { deepStrictEqual } from "node:assert/strict";
import { PendingReturns } from "./store.ts";

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
});
