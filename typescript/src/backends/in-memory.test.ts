import { suite, test } from "node:test";
import { InMemoryCache, InMemoryQueue, InMemoryStore } from "./in-memory.ts";
import { cacheContractTest, queueContractTest, storeContractTest, } from "../store.test.ts";
import { deepStrictEqual, rejects, strictEqual } from "node:assert/strict";

await suite(import.meta.filename, async () => {
  await test(InMemoryStore.name, async () => {
    await storeContractTest(() => new InMemoryStore());
    await cacheContractTest(() => new InMemoryCache());
  });

  await test(InMemoryQueue.name, async () => {
    await queueContractTest((topics) => new InMemoryQueue(topics));
  });

  await test("join behavior", {only: true}, async () => {
    const topics = ["topic-1"];
    const queue = new InMemoryQueue(topics);

    await queue.join();

    await queue.push("topic-1", { body: "task" });

    const joinPromise = queue.join();
    const popPromise = queue.pop("topic-1");
    const result = await popPromise;
    deepStrictEqual(result, { kind: "Ok", value: { body: "task" } });
    console.log(joinPromise)
    // await joinPromise;
  });
});


await suite("InMemoryQueue specific tests", { skip: true }, async () => {
  await test("error handling for non-existent topics", async () => {
    const topics = ["existing-topic"];
    const queue = new InMemoryQueue(topics);

    const nonExistentTopic = "non-existent-topic";

    await rejects(
      queue.pop(nonExistentTopic),
      Error,
      "Cloud not find topic: non-existent-topic",
    );

    await rejects(
      queue.push(nonExistentTopic, { body: "test" }),
      Error,
      "Cloud not find topic: non-existent-topic",
    );
  });

  await test("large number of topics", async () => {
    const topicCount = 1000;
    const topics = Array.from({ length: topicCount }, (_, i) => `topic-${i}`);
    const queue = new InMemoryQueue(topics);

    // Test operations on a few random topics
    const testTopics = ["topic-0", "topic-500", "topic-999"];

    for (const topic of testTopics) {
      await queue.push(topic, { body: `message-${topic}` });
      const result = await queue.pop(topic);
      deepStrictEqual(result, {
        kind: "Ok",
        value: { body: `message-${topic}` },
      });
    }
  });

  await test("rapid topic switching", async () => {
    const topics = ["topic-a", "topic-b", "topic-c"];
    const queue = new InMemoryQueue(topics);

    const operations = [];

    // Rapidly switch between topics
    for (let i = 0; i < 100; i++) {
      const topic = topics[i % topics.length];
      operations.push(queue.push(topic, { body: `msg-${i}` }));
      operations.push(queue.pop(topic));
    }

    await Promise.all(operations);

    // All topics should be empty
    for (const topic of topics) {
      const result = await queue.pop(topic);
      strictEqual(result.kind, "QueueIsEmpty");
    }
  });

  await test("shutdown and join integration", async () => {
    const topics = ["topic-1"];
    const queue = new InMemoryQueue(topics);

    // Add a task
    await queue.push("topic-1", { body: "task" });

    // Start shutdown
    const closePromise = queue.close();

    // Start join
    const joinPromise = queue.join();

    // Process the task
    const result = await queue.pop("topic-1");
    deepStrictEqual(result, { kind: "Ok", value: { body: "task" } });

    // Both close and join should complete
    await Promise.all([closePromise, joinPromise]);
  });

  await test("timeout property consistency", async () => {
    const topics = ["topic-1"];
    const queue = new InMemoryQueue(topics);

    // Timeout should always be 10
    strictEqual(queue.timeout, 10);

    // Should remain constant after operations
    await queue.push("topic-1", { body: "test" });
    strictEqual(queue.timeout, 10);

    await queue.pop("topic-1");
    strictEqual(queue.timeout, 10);

    await queue.close();
    strictEqual(queue.timeout, 10);
  });
});
