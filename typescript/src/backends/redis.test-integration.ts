import { suite, test } from "node:test";
import { ok, strictEqual } from "node:assert/strict";
import { env } from "node:process";
import { createClientPool } from "redis";
import { Redis } from "./redis.ts";
import { cacheContractTest } from "../store.test.ts";
import { matrixSuite } from "../fixture.test.ts";

await suite(import.meta.filename, async () => {
  async function acquireResource() {
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
    const redis = new Redis(client);
    await redis.connect();
    return {
      cache: redis,
      async [Symbol.asyncDispose]() {
        await redis.client.flushAll();
        redis.destroy();
      },
    };
  }

  await cacheContractTest(acquireResource);

  await matrixSuite("as message queue", async (_, matrix) => {
    const message = "some-message";

    await test("push & pop message", async () => {
      await using resource = await acquireResource();
      await resource.cache.push(matrix.topic, message);
      strictEqual(await resource.cache.pop(matrix.topic), message);
    });

    await test("FIFO", async () => {
      await using resource = await acquireResource();
      const messages = ["first", "second", "third"];
      for (const message of messages) {
        await resource.cache.push(matrix.topic, message);
      }
      for (const message of messages) {
        strictEqual(await resource.cache.pop(matrix.topic), message);
      }
    });

    await test("pop before push", async () => {
      await using resource = await acquireResource();
      const popped = resource.cache.pop(matrix.topic);
      await resource.cache.push(matrix.topic, message);
      strictEqual(await popped, message);
    });
  });
});
