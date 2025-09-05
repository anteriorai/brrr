#!/usr/bin/env node
import {
  ActiveWorker,
  AppWorker,
  Dynamo,
  NaiveJsonCodec,
  Redis,
  Server,
  taskFn,
} from "brrr";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { createClientPool } from "redis";
import { env } from "node:process";

async function createDynamo(): Promise<Dynamo> {
  const tableName = process.env.DYNAMODB_TABLE_NAME || "brrr";
  const client = new DynamoDBClient();
  return new Dynamo(client, tableName);
}

async function createRedis(): Promise<Redis> {
  const client = createClientPool({
    RESP: 3,
    ...(env.BRRR_DEMO_REDIS_URL && { url: env.BRRR_DEMO_REDIS_URL }),
  });
  return new Redis(client);
}

// TypeScript demo is worker only
const dynamo = await createDynamo();
const redis = await createRedis();

function sum({ values }: { values: number[] }): number {
  return values.reduce((a, b) => a + b);
}

// fib and lucas share the same arg type
type Arg = { n: number; salt: string | null };

/**
 * Lucas number: lucas(n) = fib(n - 1) + fib(n + 1)
 * https://en.wikipedia.org/wiki/Lucas_number
 */
async function lucas(app: ActiveWorker, { n, salt }: Arg): Promise<number> {
  if (n < 2) {
    return 2 - n;
  }
  return app.call(sum)({
    values: await app.gather(
      app.call<[Arg], number>("fib", "brrr-demo-main")({ n: n - 1, salt }),
      app.call<[Arg], number>("fib", "brrr-demo-main")({ n: n + 1, salt }),
    ),
  });
}

const server = new Server(dynamo, redis, {
  async emit(topic: string, callId: string): Promise<void> {
    await redis.push(topic, callId);
  },
});

const codec = new NaiveJsonCodec();

const app = new AppWorker(codec, server, {
  sum: taskFn(sum),
  lucas,
});

const topic = "brrr-ts-demo-main";

await server.loop(topic, app.handle, async () => {
  return await redis.pop(topic);
});
