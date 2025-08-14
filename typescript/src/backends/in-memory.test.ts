import { suite, test } from "node:test";
import { InMemoryCache, InMemoryQueue, InMemoryStore } from "./in-memory.ts";
import {
  cacheContractTest,
  queueContractTest,
  storeContractTest,
} from "../store.test.ts";
import { deepStrictEqual, rejects, strictEqual } from "node:assert/strict";

await suite(import.meta.filename, async () => {
  await test(InMemoryStore.name, async () => {
    await storeContractTest(() => new InMemoryStore());
    await cacheContractTest(() => new InMemoryCache());
  });

  await test(InMemoryQueue.name, async () => {
    await queueContractTest((topics) => new InMemoryQueue(topics));

    await test("join", { only: true }, async () => {
      const topics = ["topic-1"];
      const queue = new InMemoryQueue(topics);

      await queue.join();

      await queue.push("topic-1", { body: "task" });

      const join = queue.join();
      const pop = queue.pop("topic-1");
      const result = await pop;
      deepStrictEqual(result, { kind: "Ok", value: { body: "task" } });
      await join;
    });
  });
});
