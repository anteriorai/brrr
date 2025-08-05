import { describe, test } from "node:test";
import {
  cacheContractTest,
  queueContractTest,
} from "../store.test.ts";
import { Redis } from "./redis.ts";
import { createClient } from "redis";

await describe(import.meta.filename, async () => {
  async function createRedisQueue() {
    const client = createClient({
      RESP: 3
    });
    await client.connect();
    return new Redis(client);
  }

  await test(Redis.name, async () => {
    await queueContractTest(createRedisQueue);
    await cacheContractTest(createRedisQueue)
  });
});
