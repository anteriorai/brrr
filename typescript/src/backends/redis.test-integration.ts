import { suite } from "node:test";
import { cacheContractTest } from "../store.test.ts";
import { ok } from "node:assert/strict";
import { env } from "node:process";
import {
  createClient,
  type RedisFunctions,
  type RedisModules,
  type RedisScripts,
} from "redis";
import { Redis } from "./redis.ts";

await suite(import.meta.filename, async () => {
  ok(env.BRRR_TEST_REDIS_URL);

  const client = createClient<
    RedisModules,
    RedisFunctions,
    RedisScripts,
    3,
    {}
  >({
    url: env.BRRR_TEST_REDIS_URL,
  });

  let redis: Redis;

  await cacheContractTest(
    async () => {
      redis = new Redis(client);
      await redis.connect();
      await client.flushAll();
      return redis;
    },
    async () => {
      await redis.close();
    },
  );
});
