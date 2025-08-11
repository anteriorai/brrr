import type { Queue } from "../queue.ts";
import {
  CompareMismatchError,
  NotFoundError,
  QueueIsClosedError,
  QueueIsEmptyError,
  UnknownTopicError,
} from "../errors.ts";
import type { Cache, MemKey, Store } from "../store.ts";
import { AsyncQueue } from "../lib/async-queue.ts";
import { clearTimeout, setTimeout } from "node:timers";

export class InMemoryQueue implements Queue {
  public readonly timeout = 10;

  private readonly queues: Map<string, AsyncQueue<string>>;

  private closing = false;
  private flushing = false;

  public constructor(topics: string[]) {
    this.queues = new Map(topics.map((topic) => [topic, new AsyncQueue()]));
  }

  public async close() {
    if (this.closing) {
      throw new QueueIsClosedError();
    }
    this.closing = true;
    for (const [_, queue] of this.queues) {
      queue.shutdown();
    }
  }

  public async join() {
    await Promise.all(this.queues.values().map((queue) => queue.join()));
  }

  public async pop(topic: string): Promise<string> {
    const queue = this.getTopicQueue(topic);
    let payload: string;
    if (this.flushing) {
      try {
        payload = queue.popSync();
      } catch (err) {
        if (err instanceof QueueIsEmptyError) {
          queue.shutdown();
          throw new QueueIsClosedError();
        }
        throw err;
      }
    } else {
      const timeout = setTimeout(() => {
        throw new QueueIsEmptyError();
      }, this.timeout);
      try {
        payload = await queue.pop();
      } finally {
        clearTimeout(timeout);
      }
    }
    queue.done();
    return payload;
  }

  public async push(topic: string, message: string): Promise<void> {
    await this.getTopicQueue(topic).push(message);
  }

  public flush() {
    this.flushing = true;
  }

  private getTopicQueue(topic: string): AsyncQueue<string> {
    const queue = this.queues.get(topic);
    if (!queue) {
      throw new Error(`Could not find topic: ${topic}`);
    }
    return queue;
  }
}

export class InMemoryByteStore implements Store, Cache {
  private innerStore = new Map<string, Uint8Array>();
  private innerCache = new Map<string, number>();

  public async compareAndDelete(
    key: MemKey,
    expected: Uint8Array,
  ): Promise<boolean> {
    const keyStr = this.key2str(key);
    const value = this.store.get(keyStr);
    if (!value || !this.isEqualBytes(value, expected)) {
      return false;
    }
    this.store.delete(keyStr);
    return true;
  }

  public async compareAndSet(
    key: MemKey,
    value: Uint8Array,
    expected: Uint8Array,
  ): Promise<boolean> {
    const keyStr = this.key2str(key);
    const currentValue = this.store.get(keyStr);
    if (!currentValue || !this.isEqualBytes(currentValue, expected)) {
      return false;
    }
    this.store.set(keyStr, value);
    return true;
  }

  public async delete(key: MemKey): Promise<boolean> {
    const keyStr = this.key2str(key);
    return this.store.delete(keyStr);
  }

  public async get(key: MemKey): Promise<Uint8Array | undefined> {
    const keyStr = this.key2str(key);
    return this.store.get(keyStr);
  }

  public async has(key: MemKey): Promise<boolean> {
    const keyStr = this.key2str(key);
    return this.store.has(keyStr);
  }

  public async set(key: MemKey, value: Uint8Array): Promise<void> {
    const keyStr = this.key2str(key);
    this.store.set(keyStr, value);
  }

  public async setNewValue(key: MemKey, value: Uint8Array): Promise<boolean> {
    const keyStr = this.key2str(key);
    if (this.store.has(keyStr)) {
      return false;
    }
    this.store.set(keyStr, value);
    return true;
  }

  private key2str(key: MemKey): string {
    return `${key.type}/${key.callHash}`;
  }

  private key2str(key: MemKey): string {
    return `${key.type}/${key.callHash}`;
  }

  private isEqualBytes(a: Uint8Array, b: Uint8Array): boolean {
    return a.length === b.length && a.every((it, i) => it === b[i]);
  }
}

export class InMemoryCache implements Cache {
  private readonly cache = new Map<string, number>();

  public async incr(key: string): Promise<number> {
    const next = (this.cache.get(key) ?? 0) + 1;
    this.cache.set(key, next);
    return next;
  }
}
