import { suite, test } from "node:test";
import { NaiveCodec } from "./naive-codec.ts";
import { codecContractTest } from "./codec.test.ts";

await suite(import.meta.filename, async () => {
  await test(NaiveCodec.name, async () => {
    await codecContractTest(new NaiveCodec());
  });
});
