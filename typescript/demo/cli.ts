#!/usr/bin/env node
import {
  AppWorker,
  type Call,
  type Codec,
  Dynamo,
  Redis,
  Server,
  taskFn,
} from "brrr";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { createClientPool } from "redis";
import { env } from "node:process";
import { type BinaryToTextEncoding, createHash } from "node:crypto";
import { TextDecoder, TextEncoder } from "node:util";

async function createDynamo(): Promise<Dynamo> {
  const tableName = process.env.DYNAMODB_TABLE_NAME || "brrr";
  const client = new DynamoDBClient();
  return new Dynamo(client, tableName);
}

async function createRedis(): Promise<Redis> {
  const client = createClientPool(
    {
      RESP: 3,
      ...(env.BRRR_DEMO_REDIS_URL && { url: env.BRRR_DEMO_REDIS_URL }),
    },
    {
      cleanupDelay: 0,
    },
  );
  return new Redis(client);
}

class JsonKwargsCodec implements Codec {
  public static readonly algorithm = "sha256";
  public static readonly binaryToTextEncoding =
    "hex" satisfies BinaryToTextEncoding;

  private static readonly encoder = new TextEncoder();
  private static readonly decoder = new TextDecoder();

  public async decodeReturn(_: string, payload: Uint8Array): Promise<unknown> {
    const decoded = JsonKwargsCodec.decoder.decode(payload);
    return JSON.parse(decoded);
  }

  public async encodeCall<A extends unknown[]>(
    taskName: string,
    args: A,
  ): Promise<Call> {
    const data = JSON.stringify(args);
    const payload = JsonKwargsCodec.encoder.encode(data);
    const callHash = await this.hashCall(taskName, args);
    return { taskName, payload, callHash };
  }

  public async invokeTask<A extends unknown[], R>(
    call: Call,
    task: (...args: A) => Promise<R>,
  ): Promise<Uint8Array> {
    const decoded = JsonKwargsCodec.decoder.decode(call.payload);
    console.log(decoded);
    const args: A = JSON.parse(decoded);
    const result = await task(...args);
    const resultJson = JSON.stringify(result);
    return JsonKwargsCodec.encoder.encode(resultJson);
  }

  private async hashCall<A extends unknown>(
    taskName: string,
    args: A,
  ): Promise<string> {
    const data = JSON.stringify([taskName, args]);
    return createHash(JsonKwargsCodec.algorithm)
      .update(data)
      .digest(JsonKwargsCodec.binaryToTextEncoding);
  }
}

// TypeScript demo is worker only
const dynamo = await createDynamo();
const redis = await createRedis();

function sum(arg: { values: number[] }): number {
  return arg.values.reduce((a, b) => a + b);
}

const server = new Server(dynamo, redis, {
  async emit(topic: string, callId: string): Promise<void> {
    await redis.push(topic, callId);
  },
});

const app = new AppWorker(new JsonKwargsCodec(), server, {
  sum: taskFn(sum),
});

const topic = "brrr-ts-demo-main";

await server.loop(topic, app.handle, async () => {
  return await redis.pop(topic);
});
