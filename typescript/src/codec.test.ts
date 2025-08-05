import { suite, test } from "node:test";
import { ok } from "node:assert/strict";
import type { Codec } from "./codec.ts";
import { notDeepStrictEqual } from "node:assert";

export async function codecContractTest(codec: Codec) {
  await suite("store-contract", async () => {
    async function plus(a: number, b: string): Promise<number> {
      return Number(a) + Number(b);
    }

    await test("deterministic call hash", async () => {
      const a = await codec.encodeCall("foo", [1, 2]);
      const b = await codec.encodeCall("foo", [1, 2]);
      ok(a.equals(b));
    });

    await test("different arguments produce different hashes", async () => {
      const a = await codec.encodeCall("foo", [1, 2]);
      const b = await codec.encodeCall("foo", [2, 1]);
      const c = await codec.encodeCall("bar", [1, 2]);
      ok(!a.equals(b));
      ok(!a.equals(c));
      ok(!b.equals(c));
    });

    await test("round trip: encodeCall -> invokeTask -> decodeReturn", async () => {
      const args = [1, "2"];
      const call = await codec.encodeCall(plus.name, args);
      const result = await codec.invokeTask(call, plus);
      await codec.decodeReturn(plus.name, result);
    });
  });
}
