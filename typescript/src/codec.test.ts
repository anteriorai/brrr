import { suite, test } from "node:test";
import { deepStrictEqual, notDeepStrictEqual } from "node:assert/strict";
import type { Codec } from "./codec.ts";

export async function codecContractTest(codec: Codec) {
  await suite("store-contract", async () => {
    const cases: Record<string, [unknown[], unknown[]]> = {
      simple: [
        [1, 2],
        [1, 2],
      ],
      "simple-object": [[{ b: 2, a: 1 }], [{ a: 1, b: 2 }]],
      "nested object": [
        [{ b: { c: 3, a: 1 }, a: 2 }],
        [{ a: 2, b: { a: 1, c: 3 } }],
      ],
      "array of objects": [
        [
          [
            { b: 2, a: 1 },
            { d: 4, c: 3 },
          ],
        ],
        [
          [
            { a: 1, b: 2 },
            { c: 3, d: 4 },
          ],
        ],
      ],
      "complex object": [
        [{ a: 1, b: { c: 3, d: [4, 5] }, e: "6" }],
        [{ e: "6", b: { d: [4, 5], c: 3 }, a: 1 }],
      ],
      nulls: [
        [{ a: null, b: [1, 2, null], c: { d: null } }],
        [{ c: { d: null }, b: [1, 2, null], a: null }],
      ],
      undefineds: [
        [{ a: undefined, b: [1, 2, undefined], c: { d: undefined } }],
        [{ c: { d: undefined }, b: [1, 2, undefined], a: undefined }],
      ],
      empties: [[{ a: {}, b: [] }], [{ b: [], a: {} }]],
      "mixed types": [
        [{ a: 1, b: "2", c: true, d: null }],
        [{ d: null, c: true, b: "2", a: 1 }],
      ],
    } as const;

    await suite("deterministic call hash", async () => {
      for (const [name, args] of Object.entries(cases)) {
        await test(name, async () => {
          // sanity check
          deepStrictEqual(...args);

          deepStrictEqual(
            await codec.encodeCall("foo", args[0]),
            await codec.encodeCall("foo", args[1]),
          );
        });
      }
    });

    await test("different arguments produce different hashes", async () => {
      const a = await codec.encodeCall("foo", [1, 2]);
      const b = await codec.encodeCall("foo", [2, 1]);
      const c = await codec.encodeCall("bar", [1, 2]);
      notDeepStrictEqual(a, b);
      notDeepStrictEqual(a, c);
      notDeepStrictEqual(b, c);
    });
  });
}
