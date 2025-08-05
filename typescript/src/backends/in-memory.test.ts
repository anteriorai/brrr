import { suite, test } from "node:test";
import { InMemoryByteStore, InMemoryQueue } from "./in-memory.ts";
import {
  cacheContractTest,
  queueContractTest,
  storeContractTest,
} from "../store.test.ts";

await suite(import.meta.filename, async () => {
  await test(InMemoryByteStore.name, async () => {
    await storeContractTest(async () => new InMemoryByteStore());
    await cacheContractTest(async () => new InMemoryByteStore());
  });

  await test(InMemoryQueue.name, async () => {
    await queueContractTest(async (topics) => new InMemoryQueue(topics));
  });
});
