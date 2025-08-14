import { suite, test } from "node:test";
import { InMemoryCache, InMemoryQueue, InMemoryStore } from "./in-memory.ts";
import {
  cacheContractTest,
  queueContractTest,
  storeContractTest,
} from "../store.test.ts";

await suite(import.meta.filename, async () => {
  await test(InMemoryStore.name, async () => {
    await storeContractTest(() => new InMemoryStore());
    await cacheContractTest(() => new InMemoryCache());
  });

  await test(InMemoryQueue.name, async () => {
    await queueContractTest((topics) => new InMemoryQueue(topics));

    await test("join works over multiple topics", async () => {
      const topics = ["topic-1", "topic-2"];
      const queue = new InMemoryQueue(topics);
      await queue.join();
      await queue.push("topic-1", { body: "task" });
      await queue.push("topic-2", { body: "task" });
      const join = queue.join();
      await Promise.all([queue.pop("topic-1"), queue.pop("topic-2")]);
      await join;
    });
  });
});
