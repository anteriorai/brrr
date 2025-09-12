import { suite, test } from "node:test";
import { InMemoryCache, InMemoryStore } from "./in-memory.ts";
import { cacheContractTest, storeContractTest } from "../store.test.ts";

await suite(import.meta.filename, async () => {
  await test(InMemoryStore.name, async () => {
    await storeContractTest(async () => ({
      store: new InMemoryStore(),
      async [Symbol.asyncDispose]() {},
    }));
  });

  await test(InMemoryCache.name, async () => {
    await cacheContractTest(async () => ({
      cache: new InMemoryCache(),
      async [Symbol.asyncDispose]() {},
    }));
  });
});
