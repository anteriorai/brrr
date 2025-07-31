import { describe, test } from "node:test";
import { InMemoryByteStore, InMemoryQueue } from "./in-memory.ts";
import {
  cacheContractTest,
  queueContractTest,
  storeContractTest,
} from "../store.test.ts";

await describe(import.meta.filename, async () => {
  await test(InMemoryByteStore.name, async () => {
    await storeContractTest(() => new InMemoryByteStore());
    await cacheContractTest(() => new InMemoryByteStore());
  });

  await test(InMemoryQueue.name, async () => {
    await queueContractTest((topics) => new InMemoryQueue(topics));
  });
});
