import {
  type RedisClientType,
  type RedisFunctions,
  type RedisModules,
  type RedisScripts,
} from "redis";
import type { Cache } from "../store.ts";
import { InvalidMessageError } from "../errors.ts";
import { bencoder } from "../bencode.ts";
import type { Encoding } from "node:crypto";
import { TextEncoder } from "node:util";

type RedisPayload = [1, number, string];

export class Redis implements Cache {
  public static readonly encoder = new TextEncoder();
  public static readonly encoding = "utf-8" satisfies Encoding;

  public readonly timeout: number;
  public readonly client: RedisClientType<
    RedisModules,
    RedisFunctions,
    RedisScripts,
    3
  >;

  public constructor(client: typeof this.client, timeout: number = 20) {
    this.client = client;
    this.timeout = timeout;
  }

  public async connect(): Promise<void> {
    await this.client.connect();
  }

  public async push(topic: string, message: string): Promise<void> {
    const element = bencoder.encode([
      1,
      Math.floor(Date.now() / 1000),
      message,
    ] satisfies RedisPayload);
    await this.client.rPush(topic, Buffer.from(element));
  }

  public async pop(topic: string): Promise<string | undefined> {
    const response = await this.client.blPop(topic, this.timeout);
    if (!response) {
      return;
    }
    const buffer = Redis.encoder.encode(response.element);
    const chunks = bencoder.decode(buffer, Redis.encoding) as RedisPayload;
    if (
      chunks[0] !== 1 ||
      !Number.isInteger(chunks[1]) ||
      typeof chunks[2] !== "string"
    ) {
      throw new InvalidMessageError();
    }
    return chunks[2];
  }

  public async incr(key: string): Promise<number> {
    return this.client.incr(key);
  }

  public async close(): Promise<void> {
    this.client.destroy();
  }
}
