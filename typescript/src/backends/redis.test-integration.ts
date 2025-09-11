import { suite, test } from "node:test";
import { ok, strictEqual } from "node:assert/strict";
import { env } from "node:process";
import { createClientPool } from "redis";
import { Redis } from "./redis.ts";
import { cacheContractTest } from "../store.test.ts";

await suite(import.meta.filename, async () => {
  ok(env.BRRR_TEST_REDIS_URL);

  const client = createClientPool(
    {
      RESP: 3,
      url: env.BRRR_TEST_REDIS_URL,
    },
    {
      cleanupDelay: 0,
    },
  );

  await cacheContractTest(
    async () => {
      const redis = new Redis(client);
      await redis.connect();
      return redis;
    },
    async (redis) => {
      await redis[Symbol.asyncDispose]();
    },
  );

  await suite("as message queue", async () => {
    const topic = "test-topic";
    const message = "some-message";

    await test("push & pop message", async () => {
      await using redis = new Redis(client);
      await redis.push(topic, message);
      strictEqual(await redis.pop(topic), message);
    });

    await test("FIFO", async () => {
      await using redis = new Redis(client);
      const messages = ["first", "second", "third"];
      for (const message of messages) {
        await redis.push(topic, message);
      }
      for (const message of messages) {
        strictEqual(await redis.pop(topic), message);
      }
    });

    await test("pop before push", async () => {
      await using redis = new Redis(client);
      const popped = redis.pop(topic);
      await redis.push(topic, message);
      strictEqual(await popped, message);
    });
  });
});
