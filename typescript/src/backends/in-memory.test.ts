import { beforeEach, suite, test } from "node:test";
import { InMemoryCache, InMemoryQueue, InMemoryStore } from "./in-memory.ts";
import {
  cacheContractTest,
  queueContractTest,
  storeContractTest,
} from "../store.test.ts";
import { deepStrictEqual, strictEqual } from "node:assert/strict";

await suite(import.meta.filename, async () => {
  await test(InMemoryStore.name, async () => {
    await storeContractTest(() => new InMemoryStore());
  });

  await test(InMemoryCache.name, async () => {
    await cacheContractTest(() => new InMemoryCache());
  });

  await test(InMemoryQueue.name, async () => {
    await queueContractTest((topics) => new InMemoryQueue(topics));

    const topic = "test-topic";
    let queue: InMemoryQueue;

    beforeEach(() => {
      queue = new InMemoryQueue([topic]);
    });

    await test("flushing automatically closes when empty", async () => {
      queue.flush();
      await queue.push(topic, { body: "a" });
      await queue.push(topic, { body: "b" });
      deepStrictEqual(await queue.pop(topic), {
        kind: "Ok",
        value: { body: "a" },
      });
      deepStrictEqual(await queue.pop(topic), {
        kind: "Ok",
        value: { body: "b" },
      });
      const result = await queue.pop(topic);
      strictEqual(result.kind, "QueueIsClosed");
    });
  });
});
