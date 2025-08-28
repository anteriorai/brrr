import { type RedisClientType, type RedisFunctions, type RedisModules, type RedisScripts, } from "redis";
import type { Cache } from "../store.ts";
import { InvalidMessageError, } from "../errors.ts";
import { bencoder } from "../bencode.ts";
import type { Encoding } from "node:crypto";

type RedisPayload = [number, number, string];

export class Redis implements Cache {
  public static readonly encoding = "utf-8" satisfies Encoding;

  public readonly timeout: number = 10_000
  public readonly client: RedisClientType<
    RedisModules,
    RedisFunctions,
    RedisScripts,
    3,
    {}
  >;

  public constructor(client: typeof this.client) {
    this.client = client;
  }

  public async connect(): Promise<RedisClientType<RedisModules, RedisFunctions, RedisScripts, 3, {}>> {
    return this.client.connect()
  }

  public async push(topic: string, message: string): Promise<void> {
    if (!this.client.isOpen) {
      return
    }
    const element = bencoder
      .encode([
        1,
        Math.floor(Date.now() / 1000),
        message,
      ] satisfies RedisPayload)
      .toString();
    await this.client.rPush(topic, element);
  }

  public async pop(topic: string): Promise<string | undefined> {
    const response = await this.client.blPop(topic, this.timeout);
    if (!response) {
      return;
    }
    const data = Uint8Array.from(response.element);
    const chunks = bencoder.decode(data, Redis.encoding) as RedisPayload;
    if (
      chunks[0] !== 1 ||
      Number.isInteger(chunks[1]) ||
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
    await this.client.quit();
  }
}
