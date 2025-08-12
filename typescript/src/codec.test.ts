import { suite, test } from "node:test";
import { deepStrictEqual, strictEqual } from "node:assert/strict";
import type { Codec } from "./codec.ts";
import { notDeepStrictEqual } from "node:assert";

export async function codecContractTest(codec: Codec) {
  await suite("store-contract", async () => {
    async function plus(a: number, b: string): Promise<number> {
      return a + Number.parseInt(b);
    }

    await test("deterministic call hash", async () => {
      const a = await codec.encodeCall("foo", [1, 2]);
      const b = await codec.encodeCall("foo", [1, 2]);
      deepStrictEqual(a, b);
    });

    await suite(
      "round trip: encodeCall -> invokeTask -> decodeReturn",
      async () => {
        async function identify<T>(a: T): Promise<T> {
          return a;
        }

        for (const [name, args] of Object.entries(cases)) {
          await test(name, async () => {
            const call = await codec.encodeCall(identify.name, [args[0]]);
            const result = await codec.invokeTask(call, identify);
            const decoded = await codec.decodeReturn(identify.name, result);
            deepStrictEqual(decoded, await identify(args[1]));
          });
        }
      },
    );

    await test("different arguments produce different hashes", async () => {
      const a = await codec.encodeCall("foo", [1, 2]);
      const b = await codec.encodeCall("foo", [2, 1]);
      const c = await codec.encodeCall("bar", [1, 2]);
      notDeepStrictEqual(a, b);
      notDeepStrictEqual(a, c);
      notDeepStrictEqual(b, c);
    });

    await test("round trip: encodeCall -> invokeTask -> decodeReturn", async () => {
      const args = [1, "2"];
      const call = await codec.encodeCall(plus.name, args);
      const result = await codec.invokeTask(call, plus);
      strictEqual(await codec.decodeReturn(plus.name, result), 3);
    });
  });
}
