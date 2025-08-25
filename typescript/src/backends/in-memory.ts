import type { Message, Queue, QueuePopResult } from "../queue.ts";
import type { Cache, MemKey, Store } from "../store.ts";
import { AsyncQueue } from "../lib/async-queue.ts";

export class InMemoryQueue implements Queue {
  public readonly timeout = 10;

  private closing = false;
  private flushing = false;

  private readonly queues: Map<string, AsyncQueue<Message>>;

  public constructor(topics: string[]) {
    this.queues = new Map(topics.map((topic) => [topic, new AsyncQueue()]));
  }

  public async close(): Promise<void> {
    if (this.closing) {
      return;
    }
    this.closing = true;
    for (const queue of this.queues.values()) {
      queue.shutdown();
    }
  }

  public async join(): Promise<void> {
    await Promise.all(this.queues.values().map((queue) => queue.join()));
  }

  public async pop(topic: string): Promise<QueuePopResult<Message>> {
    const queue = this.getTopicQueue(topic);
    if (this.flushing) {
      const result = queue.popSync();
      if (result.kind === "QueueIsEmpty") {
        queue.shutdown();
        return { kind: "QueueIsClosed" };
      }
      return result;
    }
    return queue.pop(this.timeout);
  }

  public flush(): void {
    this.flushing = true;
  }

  public async push(topic: string, message: Message): Promise<boolean> {
    return this.getTopicQueue(topic).push(message);
  }

  private getTopicQueue(topic: string): AsyncQueue<Message> {
    const queue = this.queues.get(topic);
    if (!queue) {
      throw new Error(`Could not find topic: ${topic}`);
    }
    return queue;
  }
}

export class InMemoryStore implements Store {
  private store = new Map<string, Uint8Array>();

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
