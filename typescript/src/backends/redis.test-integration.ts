import { afterEach, beforeEach, suite, test } from "node:test";
import { cacheContractTest } from "../store.test.ts";
import { ok, strictEqual } from "node:assert/strict";
import { env } from "node:process";
import { createClientPool } from "redis";
import { Redis } from "./redis.ts";

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

  let redis: Redis;

  async function createRedis() {
    redis = new Redis(client);
    await redis.connect();
    return redis;
  }

  async function closeRedis() {
    await client.flushAll();
    await redis.close();
  }

  await cacheContractTest(createRedis, closeRedis);

  await suite("as message queue", async () => {
    const topic = "test-topic";
    const message = "some-message";

    beforeEach(createRedis);
    afterEach(closeRedis);

    await test("push & pop message", async () => {
      await redis.push(topic, message);
      strictEqual(await redis.pop(topic), message);
    });

    await test("FIFO", async () => {
      const messages = ["first", "second", "third"];
      for (const message of messages) {
        await redis.push(topic, message);
      }
      for (const message of messages) {
        strictEqual(await redis.pop(topic), message);
      }
    });

    await test("pop before push", { only: true }, async () => {
      const popped = redis.pop(topic);
      await redis.push(topic, message);
      strictEqual(await popped, message);
    });
  });
});
