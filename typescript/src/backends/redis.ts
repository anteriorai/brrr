import {
  type RedisClientPoolType,
  type RedisFunctions,
  type RedisModules,
  type RedisScripts,
} from "redis";
import type { Cache } from "../store.ts";
import { RedisMessage, TaggedTuple } from "../tagged-tuple.ts";

export class Redis implements Cache {
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

  public async push(topic: string, content: string): Promise<void> {
    const scheduledAt = Math.floor(Date.now() / 1000);
    const message = new RedisMessage(scheduledAt, content);
    await this.client.rPush(topic, TaggedTuple.encodeToString(message));
  }

  public async pop(
    topic: string,
    timeoutMs: number = 20_000,
  ): Promise<string | undefined> {
    const response = await this.client.blPop(topic, timeoutMs / 1000);
    if (!response?.element) {
      return;
    }
    return TaggedTuple.decodeFromString(RedisMessage, response.element).content;
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
