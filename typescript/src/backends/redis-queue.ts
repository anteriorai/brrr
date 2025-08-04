import type { RedisClientType } from "redis";
import type { Queue } from "../queue.ts";
import type { Cache } from "../store.ts";
import {
  InvalidMessageError,
  QueueIsClosedError,
  QueueIsEmptyError,
} from "../errors.ts";
import { bencoder } from "../bencode.ts";
import type { Encoding } from "node:crypto";

type RedisQueuePayload = [number, number, string];

export class RedisQueue implements Queue, Cache {
  public static readonly encoding = 'utf-8' satisfies Encoding
  private readonly timeout = 20;
  private readonly client: RedisClientType;

  public constructor(client: RedisClientType) {
    this.client = client;
  }

  public async put(topic: string, message: string): Promise<void> {
    if (!this.client.isOpen) {
      throw new QueueIsClosedError();
    }
    const val = bencoder.encode([1, Math.floor(Date.now() / 1000), message] satisfies RedisQueuePayload);
    await this.client.rPush(topic, val);
  }

  public async get(topic: string): Promise<string> {
    const response = await this.client.blPop(topic, this.timeout);
    if (!response) {
      throw new QueueIsEmptyError()
    }
    const data = Uint8Array.from(response.element)
    const chunks = bencoder.decode(data, RedisQueue.encoding) as RedisQueuePayload
    if (chunks[0] !== 1 || Number.isInteger(chunks[1]) || typeof chunks[2] !== "string") {
      throw new InvalidMessageError(chunks)
    }
    return chunks[2]
  }

  public async incr(key: string): Promise<number> {
    return this.client.incr(key);
  }

  public async close(): Promise<void> {
    await this.client.quit();
  }
}
