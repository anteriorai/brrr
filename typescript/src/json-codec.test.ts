import { suite, test } from "node:test";
import { JsonCodec } from "./json-codec.ts";
import { codecContractTest } from "./codec.test.ts";

await suite(import.meta.filename, async () => {
  await test(JsonCodec.name, async () => {
    await codecContractTest(new JsonCodec());
  });
});
