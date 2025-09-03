import {
  type RedisClientPoolType,
  type RedisFunctions,
  type RedisModules,
  type RedisScripts,
  RESP_TYPES,
} from "redis";
import type { Cache } from "../store.ts";
import { InvalidMessageError } from "../errors.ts";
import { bencoder } from "../bencode.ts";
import type { Encoding } from "node:crypto";

type RedisPayload = [1, number, string];

export class Redis implements Cache {
  public static readonly encoding = "utf-8" satisfies Encoding;

  public readonly client: RedisClientPoolType<
    RedisModules,
    RedisFunctions,
    RedisScripts,
    3
  >;

  public constructor(client: typeof this.client) {
    this.client = client;
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

  public async pop(
    topic: string,
    timeout: number = 20,
  ): Promise<string | undefined> {
    const response = await this.client
      .withTypeMapping({
        [RESP_TYPES.BLOB_STRING]: Buffer,
      })
      .blPop(topic, timeout);
    if (!response) {
      return;
    }
    const chunks = bencoder.decode(
      response.element,
      Redis.encoding,
    ) as RedisPayload;
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

  public destroy(): void {
    this.client.destroy();
  }

  public async close(): Promise<void> {
    await this.client.close();
  }
}
