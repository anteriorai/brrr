#!/usr/bin/env node
import { type ActiveWorker, AppWorker, Dynamo, JsonCodec, Redis, Server, taskFn } from "brrr";
import {
  createClient,
  type RedisClientOptions,
  type RedisFunctions,
  type RedisModules,
  type RedisScripts
} from "redis";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";

const topic = {
  main: "brrr-demo-main",
  side: "brrr-demo-side",
} as const;

const env = {
  redisUrl: process.env["BRRR_DEMO_REDIS_URL"],
  dynamoTableName: process.env["DYNAMODB_TABLE_NAME"] || "brrr",
  bind: {
    addr: process.env["BRRR_DEMO_LISTEN_HOST"] || "127.0.0.1",
    port: Number.parseInt(process.env["BRRR_DEMO_LISTEN_PORT"] || "8080"),
  },
} as const;

async function fib(
  app: ActiveWorker,
  n: number,
  salt?: string,
): Promise<number> {
  if (n < 2) {
    return n;
  }
  const [a, b] = await app.gather(
    app.call(fib)(n - 2, salt),
    app.call(fib)(n - 1, salt),
  );
  return a + b;
}

async function fibAndPrint(app: ActiveWorker, n: string, salt?: string) {
  const f = await app.call(fib, topic.side)(Number.parseInt(n), salt);
  console.log(`fib(${n}) = ${f}`);
}

function hello(greetee: string): string {
  const greeting = `Hello, ${greetee}!`;
  console.log(greeting);
  return greeting;
}

// resources
async function createRedis(): Promise<Redis> {
  const options: RedisClientOptions<RedisModules, RedisFunctions, RedisScripts, 3> = env.redisUrl ? {
    url: env.redisUrl,
    socket: {
      keepAlive: true,
    }
  } : {}
  const redisClient = createClient(options)
  await redisClient.ping()
  return new Redis(redisClient);
}

async function createDynamo(): Promise<Dynamo> {
  const dynamoClient = new DynamoDBClient()
  return new Dynamo(dynamoClient, env.dynamoTableName)
}

async function createBrrr(reset: boolean): Promise<{
  server: Server,
  app: AppWorker
}> {
  const redis = await createRedis()
  const dynamo = await createDynamo();
  if (reset) {
    await redis.client.reset()
    await dynamo.createTable()
  }
  const codec = new JsonCodec()
  const server = new Server(redis, dynamo, redis)
  const app = new AppWorker(codec, server, {
    fibAndPrint,
    hello: taskFn(hello),
    fib
  })
  return { server, app }
}

switch (process.env.argv?.at(2)) {
  case 'brrr_worker': {
    const { server, app } = await createBrrr(false)
    await Promise.all([
      server.loop(topic.main, app.handle),
      server.loop(topic.side, app.handle),
    ])
    break;
  }
  case 'web_server': {
    const { server, app } = await createBrrr(true)
    break;
  }
  case 'schedule': {
    const [topic, job, ...args] = process.argv.slice(2) as [
      string, string, ...string[]
    ];
    const { app } = await createBrrr(false)
    await app.schedule(job, topic)(...args)
    break;
  }
  case 'reset': {
    const redis = await createRedis()
    await redis.client.flushAll()
    const dynamo = await createDynamo();
    await dynamo.deleteTable()
    break;
  }
}
