import type { Message, Queue, QueuePopResult } from "../queue.ts";
import type { Cache, MemKey, Store } from "../store.ts";
import { AsyncQueue } from "../lib/async-queue.ts";

export class InMemoryQueue implements Queue {
  public readonly timeout = 10;

  private readonly queues: Map<string, AsyncQueue<Message>>;

  public constructor(topics: string[]) {
    this.queues = new Map(topics.map((topic) => [topic, new AsyncQueue()]));
  }

  public async close() {
    for (const queue of this.queues.values()) {
      queue.shutdown();
    }
  }

  public async join(): Promise<void> {
    await Promise.all(this.queues.values().map((queue) => queue.join()));
  }

  public async pop(topic: string): Promise<QueuePopResult<Message>> {
    return this.getTopicQueue(topic).pop(this.timeout);
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
  ): Promise<void> {
    const keyStr = this.key2str(key);
    const value = this.innerStore.get(keyStr);
    if (!value || !this.isEqualBytes(value, expected)) {
      throw new CompareMismatchError(key);
    }
    this.innerStore.delete(keyStr);
  }

  public async compareAndSet(
    key: MemKey,
    value: Uint8Array,
    expected: Uint8Array,
  ): Promise<void> {
    const keyStr = this.key2str(key);
    const currentValue = this.innerStore.get(keyStr);
    if (!currentValue || !this.isEqualBytes(currentValue, expected)) {
      throw new CompareMismatchError(key);
    }
    this.innerStore.set(keyStr, value);
  }

  public async delete(key: MemKey): Promise<void> {
    const keyStr = this.key2str(key);
    if (!this.innerStore.has(keyStr)) {
      throw new NotFoundError(key);
    }
    this.innerStore.delete(keyStr);
  }

  public async get(key: MemKey): Promise<Uint8Array> {
    const keyStr = this.key2str(key);
    const value = this.innerStore.get(keyStr);
    if (!value) {
      throw new NotFoundError(key);
    }
    return value;
  }

  public async has(key: MemKey): Promise<boolean> {
    const keyStr = this.key2str(key);
    return this.innerStore.has(keyStr);
  }

  public async incr(key: string): Promise<number> {
    const current = this.innerCache.get(key) ?? 0;
    const next = current + 1;
    this.innerCache.set(key, next);
    return next;
  }

  public async set(key: MemKey, value: Uint8Array): Promise<void> {
    const keyStr = this.key2str(key);
    this.innerStore.set(keyStr, value);
  }

  public async setNewValue(key: MemKey, value: Uint8Array): Promise<void> {
    const keyStr = this.key2str(key);
    if (this.innerStore.has(keyStr)) {
      throw new CompareMismatchError(key);
    }
    this.innerStore.set(keyStr, value);
  }

  private isEqualBytes(a: Uint8Array, b: Uint8Array): boolean {
    return a.length === b.length && a.every((_, i) => a[i] === b[i]);
  }
}
