import { suite, test } from "node:test";
import { InMemoryStore, InMemoryQueue, InMemoryCache } from "./in-memory.ts";
import { cacheContractTest, queueContractTest, storeContractTest, } from "../store.test.ts";

await suite(import.meta.filename, async () => {
  await test(InMemoryStore.name, async () => {
    await storeContractTest(() => new InMemoryStore());
    await cacheContractTest(() => new InMemoryCache());
  });

  await test(InMemoryQueue.name, async () => {
    await queueContractTest((topics) => new InMemoryQueue(topics));
  });
});
